using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Relayer.Transports;

namespace Relayer.AzureServiceBus;

/// <summary>Options for the AzureServiceBus transport. Native client configuration is passed through unchanged (docs/decisions.md D1).</summary>
public sealed class ServiceBusTransportOptions
{
    /// <summary>Connection string for a client owned and disposed by Relayer. Do not also set Client.</summary>
    public string? ConnectionString
    {
        get; set;
    }

    /// <summary>A caller-owned client, including clients constructed with TokenCredential. Relayer never disposes it.</summary>
    public ServiceBusClient? Client
    {
        get; set;
    }

    /// <summary>Native client options, passed straight to <see cref="ServiceBusClient"/>.</summary>
    public ServiceBusClientOptions ClientOptions
    {
        get; set;
    } = new();

    /// <summary>Caller-owned administration client, used only in Provision mode. Required for caller-owned messaging clients.</summary>
    public ServiceBusAdministrationClient? AdministrationClient
    {
        get; set;
    }

    /// <summary>Native queue declarations. Names must match send destinations.</summary>
    public IList<CreateQueueOptions> Queues
    {
        get;
    } = [];

    /// <summary>
    /// Entity names whose native RequiresSession=true declarations are trusted in Validate and None modes.
    /// Use a queue name, "topic/Subscriptions/subscription", or a declared topic name to assert that every subscription
    /// on that topic, including ones other applications own, requires sessions. Provision still verifies native settings.
    /// This assertion does not enable sessions at the broker.
    /// </summary>
    public ISet<string> TrustedSessionEntities
    {
        get;
    } = new HashSet<string>(StringComparer.Ordinal);

    /// <summary>Native topic declarations. Names must match send destinations.</summary>
    public IList<CreateTopicOptions> Topics
    {
        get;
    } = [];

    /// <summary>
    /// Sets native <c>Subject</c> (<c>sys.Label</c>) to the CloudEvents type, for example <c>orders.placed;v=1</c>, so
    /// subscription filters can match the message type. Applies to direct and outbox sends; a native callback's Subject wins.
    /// Raw-envelope routes have no type and leave Subject unset. Defaults to true.
    /// </summary>
    public bool SubjectFromMessageType
    {
        get; set;
    } = true;

    /// <summary>
    /// Starts the native processors with the host. Defaults to true. When false, the host composes and validates the
    /// consumers but leaves their processors stopped until the keyed <see cref="Relayer.Transports.IConsumerControl"/>
    /// for this transport starts them, for example once provisioned entities exist. Sending works meanwhile.
    /// </summary>
    public bool AutoStartConsumers
    {
        get; set;
    } = true;

    /// <summary>Native subscriptions to provision independently of receive composition.</summary>
    public IList<CreateSubscriptionOptions> Subscriptions
    {
        get;
    } = [];

    // TopologyMode.None: entities the routes and consumers name but these options do not declare are implied, in a copy,
    // so the caller's options (and exporters reading them) are unchanged. Explicit declarations always win.
    internal ServiceBusTransportOptions WithImpliedDeclarations(IEnumerable<string> destinations, IEnumerable<IConsumerDefinition> consumers)
    {
        // Copy every option here; a property added to this class without a line below is lost in None mode.
        var implied = new ServiceBusTransportOptions
        {
            ConnectionString = ConnectionString,
            Client = Client,
            ClientOptions = ClientOptions,
            AdministrationClient = AdministrationClient,
            SubjectFromMessageType = SubjectFromMessageType,
            AutoStartConsumers = AutoStartConsumers,
        };

        foreach (var queue in Queues)
        {
            implied.Queues.Add(queue);
        }

        foreach (var topic in Topics)
        {
            implied.Topics.Add(topic);
        }

        foreach (var subscription in Subscriptions)
        {
            implied.Subscriptions.Add(subscription);
        }

        foreach (var entity in TrustedSessionEntities)
        {
            implied.TrustedSessionEntities.Add(entity);
        }

        foreach (var consumer in consumers)
        {
            if (consumer is not ConsumerDefinition<ServiceBusConsumerOptions> { Options: var options })
            {
                continue;
            }

            var sessions = options.SessionProcessorOptions is not null;

            if (options.Subscription is not { } subscription)
            {
                if (!implied.Declares(consumer.Source))
                {
                    implied.Queues.Add(new(consumer.Source)
                    {
                        RequiresSession = sessions
                    });
                }

                continue;
            }

            if (!implied.Declares(consumer.Source))
            {
                implied.Topics.Add(new(consumer.Source));
            }

            if (!implied.Subscriptions.Any(item => item.TopicName == consumer.Source && item.SubscriptionName == subscription))
            {
                implied.Subscriptions.Add(new(consumer.Source, subscription)
                {
                    RequiresSession = sessions
                });
            }
        }
        // A trusted name asserts sessions on a whole topic; any other destination is a queue. The sender is the same for both.
        foreach (var destination in destinations)
        {
            if (!implied.Declares(destination))
            {
                if (TrustedSessionEntities.Contains(destination))
                {
                    implied.Topics.Add(new(destination));
                }
                else
                {
                    implied.Queues.Add(new(destination));
                }
            }
        }

        return implied;
    }

    private bool Declares(string entity)
    {
        return Queues.Any(queue => queue.Name == entity) || Topics.Any(topic => topic.Name == entity);
    }
}
