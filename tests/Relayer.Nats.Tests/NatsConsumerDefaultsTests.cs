using Microsoft.Extensions.DependencyInjection;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Relayer.Transports;

namespace Relayer.Nats.Tests;

public class NatsConsumerDefaultsTests
{
    [Test]
    public async Task Validate_accepts_server_and_account_limits_for_omitted_pull_consumer_settings()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        await using var broker = await NatsTopologyTests.Broker.StartAsync(token, """
            jetstream {
                limits { max_request_batch: 32, max_ack_pending: 64 }
            }
            accounts {
                server_limits {
                    jetstream: enabled
                    users: [
                        { user: admin, password: admin },
                        { user: reader, password: reader, permissions: {
                            publish: ["$JS.API.STREAM.INFO.>", "$JS.API.CONSUMER.INFO.>"],
                            subscribe: ["_INBOX.>"]
                        }}
                    ]
                }
                account_limits {
                    jetstream { max_ack_pending: 24 }
                    users: [
                        { user: account_admin, password: account_admin },
                        { user: account_reader, password: account_reader, permissions: {
                            publish: ["$JS.API.STREAM.INFO.>", "$JS.API.CONSUMER.INFO.>"],
                            subscribe: ["_INBOX.>"]
                        }}
                    ]
                }
            }
            """);

        await ValidatePullConsumerDefaultsAsync(broker.Options("admin"), broker.Options("reader"), 64, token);
        await ValidatePullConsumerDefaultsAsync(broker.Options("account_admin"), broker.Options("account_reader"), 24, token);
    }

    private static async Task ValidatePullConsumerDefaultsAsync(
        NatsOpts adminOptions, NatsOpts readerOptions, long maxAckPending, CancellationToken token)
    {
        var name = TestContext.Current!.Isolation.GetIsolatedName("server_limits");
        var stream = new StreamConfig(name, [name]);
        var consumer = new ConsumerConfig("handler");
        var explicitConsumer = new ConsumerConfig("explicit_limits")
        {
            MaxBatch = 12,
            MaxAckPending = 21
        };
        await using var provisioner = Provider(adminOptions, stream, consumer, explicitConsumer);
        var provision = await provisioner.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Provision, token);
        await Assert.That(provision.Differences).IsEmpty();

        await using var admin = new NatsConnection(adminOptions);
        var js = new NatsJSContext(admin);
        var actual = (await js.GetConsumerAsync(name, consumer.Name!, token)).Info.Config;
        await Assert.That(actual.MaxBatch).IsEqualTo(32);
        await Assert.That(actual.MaxAckPending).IsEqualTo(maxAckPending);

        await using var validator = Provider(readerOptions, stream, consumer, explicitConsumer);
        var validation = await validator.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Validate, token);
        await Assert.That(validation.Differences).IsEmpty();

        await using var driftValidator = Provider(readerOptions, stream, explicitConsumer with
        {
            MaxBatch = 13,
            MaxAckPending = 22
        });
        var drift = await driftValidator.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Validate, token);
        await Assert.That(drift.Differences.Count).IsEqualTo(2);
        await Assert.That(drift.Differences.Any(difference => difference.Contains("'max_batch' expected 13, actual 12", StringComparison.Ordinal))).IsTrue();
        await Assert.That(drift.Differences.Any(difference => difference.Contains("'max_ack_pending' expected 22, actual 21", StringComparison.Ordinal))).IsTrue();
    }

    [Test]
    public async Task Validate_accepts_the_default_pinned_client_timeout()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        var broker = await NatsSessionBroker.Current.WaitAsync(token);
        var name = TestContext.Current!.Isolation.GetIsolatedName("pinned_timeout");
        var stream = new StreamConfig(name, [name]);
        var consumer = new ConsumerConfig("handler")
        {
            PriorityPolicy = ConsumerConfigPriorityPolicy.PinnedClient,
            PriorityGroups = ["workers"]
        };
        var explicitConsumer = consumer with
        {
            Name = "explicit_timeout",
            DurableName = "explicit_timeout",
            PinnedTTL = TimeSpan.FromMinutes(1)
        };
        var options = broker.Options();
        await using var provider = Provider(options, stream, consumer, explicitConsumer);
        var transport = provider.GetRequiredService<ITransport>();
        var provision = await transport.ApplyTopologyAsync(TopologyMode.Provision, token);
        await Assert.That(provision.Differences).IsEmpty();

        await using var native = new NatsConnection(options);
        var js = new NatsJSContext(native);
        var actual = (await js.GetConsumerAsync(name, consumer.Name!, token)).Info.Config;
        await Assert.That(actual.PinnedTTL).IsEqualTo(TimeSpan.FromMinutes(2));

        var validation = await transport.ApplyTopologyAsync(TopologyMode.Validate, token);
        await Assert.That(validation.Differences).IsEmpty();

        await using var driftValidator = Provider(options, stream, explicitConsumer with
        {
            PinnedTTL = TimeSpan.FromMinutes(2)
        });
        var drift = await driftValidator.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Validate, token);
        await Assert.That(drift.Differences.Single()).Contains("'priority_timeout' expected 120000000000, actual 60000000000");
    }

    private static ServiceProvider Provider(NatsOpts nativeOptions, StreamConfig stream, params ConsumerConfig[] consumers)
    {
        return NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = nativeOptions;
            options.Streams.Add(new(stream.Name!)
            {
                Configuration = stream
            });

            foreach (var consumer in consumers)
            {
                options.Consumers.Add(new(stream.Name!, consumer.Name!)
                {
                    Configuration = consumer
                });
            }
        });
    }
}
