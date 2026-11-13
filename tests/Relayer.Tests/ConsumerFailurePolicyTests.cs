using System.Threading.Channels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Relayer.InMemory;
using Relayer.Transports;

namespace Relayer.Tests;

public class ConsumerFailurePolicyTests
{
    [Test]
    public async Task Options_pattern_binds_failure_policy()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Failure:RetryDelays:0"] = "00:00:02",
            ["Failure:RedeliveryDelays:0"] = "00:01:00",
            ["Failure:Jitter"] = "0",
            ["Failure:DeadLetterEndpoint"] = "errors",
            ["Failure:CopyTimeout"] = "00:00:10",
        }).Build();
        var services = new ServiceCollection();
        services.AddOptions<FailureOptions>().Bind(configuration.GetSection("Failure"));
        await using var provider = services.BuildServiceProvider();

        var policy = provider.GetRequiredService<IOptions<FailureOptions>>().Value;
        await Assert.That(policy.RetryDelays!.Length).IsEqualTo(1);
        await Assert.That(policy.RetryDelays[0]).IsEqualTo(TimeSpan.FromSeconds(2));
        await Assert.That(policy.RedeliveryDelays.Length).IsEqualTo(1);
        await Assert.That(policy.RedeliveryDelays[0]).IsEqualTo(TimeSpan.FromMinutes(1));
        await Assert.That(policy.Jitter).IsEqualTo(0);
        await Assert.That(policy.DeadLetterEndpoint).IsEqualTo("errors");
        await Assert.That(policy.CopyTimeout).IsEqualTo(TimeSpan.FromSeconds(10));
    }

    [Test]
    public async Task Bound_policy_replaces_default_delays_and_is_reusable_across_consumer_types()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Failure:RetryDelays:0"] = "00:00:02",
            ["Failure:RedeliveryDelays:0"] = "00:01:00",
            ["Failure:DeadLetterEndpoint"] = "errors",
            ["Failure:Jitter"] = "0",
            ["Failure:CopyTimeout"] = "00:00:10",
        }).Build();
        var policy = configuration.GetSection("Failure").Get<FailureOptions>()!;
        var classifier = new FailureClassifierBuilder().On<InvalidOperationException>(FailureAction.Discard).Build();
        var services = new ServiceCollection();
        services.AddRelayer(relayer =>
        {
            relayer.AddTransport<Options, Options, Options>("broker", transport =>
                transport.Consume("source", consumer => consumer.Failures(policy).Classify(classifier)));
            relayer.InMemory("test", transport =>
                transport.Consume("source", consumer => ((IConsumerBuilder)consumer).Failures(policy).Classify(classifier)));
        });
        await using var provider = services.BuildServiceProvider();
        var declarations = provider.GetRequiredService<RelayerConfiguration>().Transports;
        foreach (var declaration in declarations)
        {
            var consumer = declaration.Consumers[0];
            await Assert.That(consumer.Failure).IsSameReferenceAs(policy);
            await Assert.That(consumer.FailureClassifier).IsSameReferenceAs(classifier);
            await Assert.That(consumer.Failure.RetryDelays!.Length).IsEqualTo(1);
            await Assert.That(consumer.Failure.RetryDelays[0]).IsEqualTo(TimeSpan.FromSeconds(2));
            await Assert.That(consumer.Failure.RedeliveryDelays[0]).IsEqualTo(TimeSpan.FromMinutes(1));
            await Assert.That(consumer.Failure.DeadLetterEndpoint).IsEqualTo("errors");
            await Assert.That(consumer.Failure.CopyTimeout).IsEqualTo(TimeSpan.FromSeconds(10));
            await Assert.That(consumer.Failure.Jitter).IsEqualTo(0);
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Binding_missing_delays_keeps_defaults_and_preserves_invalid_zero_timeout_for_validation(bool explicitTimeout)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Failure:Jitter"] = "0",
            ["Failure:CopyTimeout"] = explicitTimeout ? "00:00:00" : null,
        }).Build();
        var policy = configuration.GetSection("Failure").Get<FailureOptions>()!;
        await Assert.That(policy.RetryDelays).IsNull();
        await Assert.That(policy.RedeliveryDelays.Length).IsEqualTo(0);
        await Assert.That(policy.CopyTimeout).IsEqualTo(explicitTimeout ? TimeSpan.Zero : (TimeSpan?)null);
    }

    [Test]
    public async Task In_memory_uses_existing_dead_letter_copy_fallback_when_native_dead_lettering_is_disabled()
    {
        var services = new ServiceCollection();
        services.AddSingleton(Channel.CreateUnbounded<int>());
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.InMemory("local", transport =>
            {
                transport.Options.Configure(options => options.Endpoints.Add("source", new() { Capabilities = new() }));
                transport.Send<PolicyMessage>("source");
                transport.Send<PolicyError>("errors");
                transport.Consume("source", consumer => consumer.Failures(new() { RetryDelays = [], DeadLetterEndpoint = "errors" }).Handler<PolicyHandler>());
            });
        });
        await using var provider = services.BuildServiceProvider();
        var native = (InMemoryTransport)provider.GetRequiredService<ITransport>();
        var copied = Channel.CreateUnbounded<InMemoryDeadLetter>();
        native.RegisterConsumer("errors", (delivery, _) =>
        {
            copied.Writer.TryWrite(new InMemoryDeadLetter(delivery, new("copy")));
            return new(Disposition.Complete);
        });
        var hosted = provider.GetServices<IHostedService>().ToArray();
        foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>()) await lifecycle.StartingAsync(default);
        foreach (var service in hosted) await service.StartAsync(default);
        try
        {
            await using var scope = provider.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new PolicyMessage());
            var copy = await copied.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(copy.Headers.ToArray().Single(pair => pair.Key == "ce_type").Value.ToString()).IsEqualTo("tests.consumer-policy;v=1");
            await Assert.That(copy.Headers.ToArray().Single(pair => pair.Key == FailureHeaders.Attempts).Value.ToString()).IsEqualTo("1");
            await Assert.That(native.DeadLetters("source").TryRead(out _)).IsFalse();
        }
        finally
        {
            foreach (var service in hosted.Reverse()) await service.StopAsync(default);
        }
    }

    [Test]
    public async Task Failure_configuration_freezes_with_the_consumer()
    {
        ConsumerBuilder<Options>? captured = null;
        var services = new ServiceCollection();
        services.AddRelayer(relayer => relayer.AddTransport<Options, Options, Options>("broker", transport =>
            transport.Consume("source", consumer => captured = consumer)));
        await Assert.That(() => captured!.Failures(FailureOptions.None)).Throws<InvalidOperationException>();
        await Assert.That(() => captured!.Classify(new DefaultFailureClassifier())).Throws<InvalidOperationException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task In_memory_executes_reused_policy_and_retains_redelivery_history_per_consumer(bool log)
    {
        var policy = new FailureOptions { RetryDelays = [TimeSpan.Zero, TimeSpan.Zero], RedeliveryDelays = [TimeSpan.Zero, TimeSpan.Zero], Jitter = 0 };
        var services = new ServiceCollection();
        services.AddSingleton(Channel.CreateUnbounded<int>());
        // The per-consumer classifier must take precedence over the global registration.
        services.AddSingleton(new FailureClassifierBuilder().On<InvalidOperationException>(FailureAction.Discard).Build());
        services.AddRelayer(relayer =>
        {
            relayer.AddMessageTypesFromThisAssembly();
            relayer.InMemory("local", transport =>
            {
                transport.Options.Configure(options => options.Endpoints.Add("source", new()
                {
                    Capabilities = new()
                    {
                        ConsumptionModel = log ? ConsumptionModel.Log : ConsumptionModel.Queue,
                        OrderedByKey = log, DeadLetter = true,
                        MaxRedeliveryDelay = TimeSpan.MaxValue, MaxDeliveryDelay = TimeSpan.MaxValue,
                    },
                }));
                transport.Send<PolicyMessage>("source");
                transport.Consume("source", consumer =>
                {
                    consumer.Options.Group = log ? "first" : null;
                    consumer.Failures(policy).Classify(new DefaultFailureClassifier()).Handler<PolicyHandler>();
                });
                if (log) transport.Consume("source", consumer =>
                {
                    consumer.Options.Group = "second";
                    consumer.Failures(policy).Classify(new DefaultFailureClassifier()).Handler<PolicyHandler>();
                });
            });
        });
        await using var provider = services.BuildServiceProvider();
        var hosted = provider.GetServices<IHostedService>().ToArray();
        foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>()) await lifecycle.StartingAsync(default);
        foreach (var service in hosted) await service.StartAsync(default);
        try
        {
            await using var scope = provider.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<ISender>().SendAsync(new PolicyMessage());
            var transport = (InMemoryTransport)provider.GetRequiredService<ITransport>();
            for (var i = 0; i < (log ? 2 : 1); i++)
            {
                var dead = await transport.DeadLetters("source").ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
                await Assert.That(dead.Error.Headers.ToArray().Single(pair => pair.Key == FailureHeaders.Attempts).Value.ToString()).IsEqualTo("9");
                await Assert.That(dead.Error.Headers.ToArray().Single(pair => pair.Key == FailureHeaders.Redeliveries).Value.ToString()).IsEqualTo("2");
            }
            var attempts = provider.GetRequiredService<Channel<int>>();
            var count = 0;
            while (attempts.Reader.TryRead(out _)) count++;
            await Assert.That(count).IsEqualTo(log ? 18 : 9);
        }
        finally
        {
            foreach (var service in hosted.Reverse()) await service.StopAsync(default);
        }
    }

    public sealed class Options;
}

[Message("tests.consumer-policy")]
public sealed record PolicyMessage;

public sealed class PolicyHandler(Channel<int> attempts) : IHandler<PolicyMessage>
{
    public ValueTask HandleAsync(PolicyMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        attempts.Writer.TryWrite(context.Attempt);
        throw new InvalidOperationException("retry me");
    }
}

[Message("tests.consumer-policy-error")]
public sealed record PolicyError;
