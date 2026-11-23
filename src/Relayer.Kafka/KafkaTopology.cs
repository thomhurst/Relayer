using Dekaf;
using Dekaf.Admin;
using Dekaf.Errors;
using Dekaf.Metadata;
using Dekaf.Protocol;

namespace Relayer.Kafka;

internal static class KafkaTopology
{
    internal static async ValueTask<TopologyReport> ApplyAsync(string name, KafkaTransportOptions? options,
        IEnumerable<string> endpoints, TopologyMode mode, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (mode == TopologyMode.None)
        {
            return new([]);
        }

        if (mode is not (TopologyMode.Validate or TopologyMode.Provision))
        {
            throw new ArgumentOutOfRangeException(nameof(mode));
        }

        var declarations = new Dictionary<string, NewTopic>(StringComparer.Ordinal);

        if (options is not null)
        {
            foreach (var topic in options.Topics)
            {
                ArgumentNullException.ThrowIfNull(topic);
                ArgumentException.ThrowIfNullOrWhiteSpace(topic.Name);

                if (!declarations.TryAdd(topic.Name, topic))
                {
                    throw new ArgumentException($"Kafka transport '{name}': duplicate topic declaration '{topic.Name}'.");
                }
            }
        }

        var topics = new SortedSet<string>(endpoints, StringComparer.Ordinal);
        topics.UnionWith(declarations.Keys);

        if (topics.Count == 0)
        {
            return new([]);
        }

        if (options is null)
        {
            throw new InvalidOperationException("Kafka topology requires native admin configuration.");
        }

        var creation = options.TopicCreationOptions ?? new CreateTopicsOptions();

        if (mode == TopologyMode.Provision && creation.ValidateOnly)
        {
            throw new ArgumentException($"Kafka transport '{name}': Provision requires TopicCreationOptions.ValidateOnly = false.");
        }

        if (mode == TopologyMode.Provision)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(creation.TimeoutMs);
        }

        var builder = new AdminClientBuilder().WithBootstrapServers(options.BootstrapServers.ToArray());
        options.ConfigureAdmin?.Invoke(builder);
        await using var admin = builder.Build();
        // Initial metadata requests list all topics and cannot trigger broker auto-creation.
        await admin.ListTopicsAsync(new ListTopicsOptions { ListInternal = true }, cancellationToken).ConfigureAwait(false);

        if (mode == TopologyMode.Validate)
        {
            return await ValidateAsync(admin, topics, declarations, cancellationToken).ConfigureAwait(false);
        }

        foreach (var topic in topics)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (admin.Metadata.GetTopic(topic) is { ErrorCode: ErrorCode.None })
            {
                continue;
            }

            var declaration = declarations.TryGetValue(topic, out var configured) ? configured : new NewTopic { Name = topic };

            try
            {
                await admin.CreateTopicsAsync([declaration], creation, cancellationToken).ConfigureAwait(false);
            }
            catch (KafkaException error) when (error.ErrorCode == ErrorCode.TopicAlreadyExists)
            {
                // Another application instance won the create. Verify its settings below.
            }
        }

        return await VerifyProvisionedAsync(builder, topics, declarations, creation.TimeoutMs, cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<TopologyReport> VerifyProvisionedAsync(AdminClientBuilder builder,
        IReadOnlyCollection<string> topics, IReadOnlyDictionary<string, NewTopic> declarations, int timeoutMs,
        CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        if (timeoutMs == 0)
        {
            deadline.Cancel();
        }
        else
        {
            deadline.CancelAfter(timeoutMs);
        }

        try
        {
            while (true)
            {
                deadline.Token.ThrowIfCancellationRequested();

                // ListTopicsAsync caches metadata. Fresh clients read all topics without targeted
                // metadata requests that can auto-create topics. See D88 and Dekaf #4190.
                await using var verification = builder.Build();
                await verification.ListTopicsAsync(new ListTopicsOptions { ListInternal = true }, deadline.Token).ConfigureAwait(false);
                var missing = false;

                foreach (var topic in topics)
                {
                    var actual = verification.Metadata.GetTopic(topic);

                    if (actual is null || actual.ErrorCode is ErrorCode.UnknownTopicOrPartition or ErrorCode.LeaderNotAvailable)
                    {
                        missing = true;
                    }
                    else if (actual.ErrorCode != ErrorCode.None)
                    {
                        throw new KafkaException(actual.ErrorCode, $"Cannot verify Kafka topic '{topic}': {actual.ErrorCode}.");
                    }
                    else if (actual.PartitionCount == 0)
                    {
                        missing = true;
                    }
                }

                if (!missing)
                {
                    return await ValidateAsync(verification, topics, declarations, deadline.Token).ConfigureAwait(false);
                }

                await Task.Delay(TimeSpan.FromMilliseconds(100), deadline.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException error) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"Kafka topics [{string.Join(", ", topics)}] were not visible for topology verification within {timeoutMs} ms. Check broker metadata access and topic creation (ConfigureAdmin).", error);
        }
    }

    private static async ValueTask<TopologyReport> ValidateAsync(IAdminClient admin, IEnumerable<string> topics,
        IReadOnlyDictionary<string, NewTopic> declarations, CancellationToken cancellationToken)
    {
        var differences = new List<string>();

        foreach (var topic in topics)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var actual = admin.Metadata.GetTopic(topic);

            if (actual is null || actual.ErrorCode != ErrorCode.None)
            {
                differences.Add($"Topic '{topic}' is missing or not visible to the admin client (metadata: {actual?.ErrorCode.ToString() ?? "missing"}). Create it externally or use Provision and grant topic metadata access (ConfigureAdmin).");
                continue;
            }

            if (!declarations.TryGetValue(topic, out var expected))
            {
                continue;
            }

            ComparePartitions(expected, actual, differences);

            if (expected.Configs is not { Count: > 0 } configs)
            {
                continue;
            }

            var resource = ConfigResource.Topic(topic);
            var resources = await admin.DescribeConfigsAsync([resource], cancellationToken: cancellationToken).ConfigureAwait(false);

            foreach (var config in configs)
            {
                var value = resources.TryGetValue(resource, out var entries)
                    ? entries.FirstOrDefault(entry => entry.Name == config.Key)?.Value : null;
                Compare(topic, config.Key, config.Value, value, differences);
            }
        }

        return new(differences);
    }

    private static void ComparePartitions(NewTopic expected, TopicInfo actual, List<string> differences)
    {
        if (expected.ReplicaAssignments is { Count: > 0 } assignments)
        {
            Compare(expected.Name, nameof(NewTopic.NumPartitions), assignments.Count, actual.PartitionCount, differences);

            foreach (var assignment in assignments)
            {
                var partition = actual.Partitions.FirstOrDefault(item => item.PartitionIndex == assignment.Key);

                if (partition is null || !assignment.Value.SequenceEqual(partition.ReplicaNodes))
                {
                    differences.Add($"Topic '{expected.Name}': partition {assignment.Key} replicas expected [{string.Join(",", assignment.Value)}], actual [{(partition is null ? "missing" : string.Join(",", partition.ReplicaNodes))}]. Reconcile externally; Relayer never modifies or deletes existing topics.");
                }
            }

            return;
        }

        if (expected.NumPartitions != -1)
        {
            Compare(expected.Name, nameof(NewTopic.NumPartitions), expected.NumPartitions, actual.PartitionCount, differences);
        }

        if (expected.ReplicationFactor != -1)
        {
            foreach (var partition in actual.Partitions)
            {
                Compare(expected.Name, $"partition {partition.PartitionIndex} {nameof(NewTopic.ReplicationFactor)}",
                    (int)expected.ReplicationFactor, partition.ReplicaNodes.Count, differences);
            }
        }
    }

    private static void Compare<T>(string topic, string property, T expected, T actual, List<string> differences)
    {
        if (!EqualityComparer<T>.Default.Equals(expected, actual))
        {
            differences.Add($"Topic '{topic}': {property} expected '{expected}', actual '{actual}'. Reconcile externally; Relayer never modifies or deletes existing topics.");
        }
    }
}
