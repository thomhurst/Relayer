using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Relayer.RabbitMQ;
using Relayer.Transports;

namespace Relayer.Tests;

public class RabbitMqTopologyTests
{
    [Test]
    public async Task Hosted_provision_preserves_native_routing_dlx_credentials_and_connection_ownership()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var broker = await Broker.StartAsync(token);
        var credentials = new NativeCredentials();
        var factory = broker.Factory();
        factory.CredentialsProvider = credentials;
        var host = new HostBuilder().ConfigureServices(services => services.AddRelayer(builder =>
            builder.RabbitMQ("rabbit", rabbit =>
            {
                rabbit.TopologyMode = TopologyMode.Provision;
                rabbit.Options.Configure(options =>
                {
                    options.ConnectionFactory = factory;
                    options.Exchanges.Add(new("orders") { Type = ExchangeType.Topic });
                    options.Exchanges.Add(new("dead"));
                    options.Queues.Add(new("orders")
                    {
                        Arguments = new Dictionary<string, object?>
                        {
                            ["x-dead-letter-exchange"] = "dead",
                            ["x-dead-letter-routing-key"] = "failed",
                        },
                    });
                    options.Queues.Add(new("dead"));
                    options.Queues.Add(new("owned") { Durable = false, Exclusive = true });
                    options.Bindings.Add(new("orders", "orders", "placed.*"));
                    options.Bindings.Add(new("dead", "dead", "failed"));
                });
            }))).Build();
        await using var hostLifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        var transport = host.Services.GetRequiredService<ITransport>();
        await Assert.That(credentials.Calls > 0).IsTrue();
        await Assert.That(factory.CredentialsProvider).IsSameReferenceAs(credentials);
        await Assert.That(factory.AutomaticRecoveryEnabled).IsTrue();
        await Assert.That(factory.TopologyRecoveryEnabled).IsTrue();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);

        await using var connection = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(new(true, true), token);
        await Assert.That(await QueueErrorAsync(connection, "owned", token)).IsEqualTo((ushort)405);
        var deadLettered = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, args) =>
        {
            deadLettered.TrySetResult(args.Body.ToArray());
            return Task.CompletedTask;
        };
        await channel.BasicConsumeAsync("dead", autoAck: true, consumer, token);
        await channel.BasicPublishAsync("orders", "placed.eu", mandatory: true, new BasicProperties { Persistent = true }, new byte[] { 42 }, token);
        var message = await channel.BasicGetAsync("orders", autoAck: false, token);
        await Assert.That(message).IsNotNull();
        await Assert.That(message!.Body.Span[0]).IsEqualTo((byte)42);
        await channel.BasicNackAsync(message.DeliveryTag, multiple: false, requeue: false, token);
        await Assert.That((await deadLettered.Task.WaitAsync(token))[0]).IsEqualTo((byte)42);

        // Native equivalence checks report both mismatches and still create unrelated missing entities.
        await using var driftProvider = RabbitMqConfigurationTests.Provider(options =>
        {
            options.ConnectionFactory = broker.Factory();
            options.Exchanges.Add(new("orders") { Type = ExchangeType.Direct });
            options.Queues.Add(new("orders") { Durable = false });
            options.Queues.Add(new("after-errors"));
        });
        var drift = await driftProvider.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Provision, token);
        await Assert.That(drift.Differences.Count).IsEqualTo(2);
        await Assert.That(drift.Differences[0]).Contains("inequivalent arg 'type'");
        await Assert.That(drift.Differences[1]).Contains("inequivalent arg 'durable'");
        await channel.QueueDeclarePassiveAsync("after-errors", token);
        // The original exchange is still topic, and the original queue and DLX arguments remain unchanged.
        await channel.ExchangeDeclareAsync("orders", ExchangeType.Topic, true, false, cancellationToken: token);
        await channel.QueueDeclareAsync("orders", true, false, false, new Dictionary<string, object?>
        {
            ["x-dead-letter-exchange"] = "dead", ["x-dead-letter-routing-key"] = "failed",
        }, cancellationToken: token);

        await host.StopAsync(token);
        await Assert.That(await QueueErrorAsync(connection, "owned", token)).IsEqualTo((ushort)404);
        // Stop closes only the adapter's connection; a caller's independent connection stays usable.
        await channel.QueueDeclarePassiveAsync("orders", token);
    }

    [Test]
    public async Task Validate_is_read_only_reports_missing_and_unverifiable_expectations_and_blocks_startup()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = timeout.Token;
        await using var broker = await Broker.StartAsync(token);
        await using var connection = await broker.Factory().CreateConnectionAsync(token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        await channel.ExchangeDeclareAsync("events", ExchangeType.Fanout, true, false, cancellationToken: token);
        await channel.QueueDeclareAsync("existing", true, false, false, cancellationToken: token);

        // AMQP passive checks need no configure or write permission and publish no probes.
        var reader = "reader-" + broker.VirtualHost;
        await broker.CommandAsync(["rabbitmqctl", "add_user", reader, "reader"], token);
        await broker.CommandAsync(["rabbitmqctl", "set_permissions", "-p", broker.VirtualHost, reader, "^$", "^$", ".*"], token);
        var readOnly = broker.Factory();
        readOnly.UserName = reader;
        readOnly.Password = "reader";
        await using var valid = RabbitMqConfigurationTests.Provider(options =>
        {
            options.ConnectionFactory = readOnly;
            options.Exchanges.Add(new("events"));
            options.Queues.Add(new("existing"));
        });
        await Assert.That((await valid.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);

        await using var missing = RabbitMqConfigurationTests.Provider(options =>
        {
            options.ConnectionFactory = readOnly;
            options.Exchanges.Add(new("absent-exchange"));
            options.Exchanges.Add(new("events"));
            options.Queues.Add(new("absent-queue"));
            options.Queues.Add(new("existing"));
        });
        var differences = (await missing.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Validate, token)).Differences;
        await Assert.That(differences.Count).IsEqualTo(2);
        await Assert.That(differences[0]).Contains("absent-exchange");
        await Assert.That(differences[1]).Contains("absent-queue");
        await Assert.That(await QueueErrorAsync(connection, "absent-queue", token)).IsEqualTo((ushort)404);
        await Assert.That((await channel.QueueDeclarePassiveAsync("existing", token)).MessageCount).IsEqualTo(0u);

        await using var unverifiable = RabbitMqConfigurationTests.Provider(options =>
        {
            options.ConnectionFactory = readOnly;
            options.Exchanges.Add(new("events") { Type = ExchangeType.Fanout });
            options.Queues.Add(new("existing") { Durable = true });
            options.Bindings.Add(new("existing", "events", ""));
        });
        var unresolved = (await unverifiable.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Validate, token)).Differences;
        await Assert.That(unresolved.Count).IsEqualTo(3);
        await Assert.That(unresolved[0]).Contains("cannot verify");
        await Assert.That(unresolved[1]).Contains("cannot verify");
        await Assert.That(unresolved[2]).Contains("cannot inspect binding");

        var host = new HostBuilder().ConfigureServices(services => services.AddRelayer(builder =>
            builder.RabbitMQ("missing", rabbit => rabbit.Options.Configure(options =>
            {
                options.ConnectionFactory = broker.Factory();
                options.Queues.Add(new("absent-queue"));
            })))).Build();
        await using var hostLifetime = (IAsyncDisposable)host;
        var failure = await Assert.That(async () => await host.StartAsync(token)).Throws<OptionsValidationException>();
        await Assert.That(failure!.Message).Contains("Transport 'missing' (Validate)");
        await Assert.That(failure.Message).Contains("absent-queue");
    }

    private static async Task<ushort> QueueErrorAsync(IConnection connection, string queue, CancellationToken token)
    {
        await using var channel = await connection.CreateChannelAsync(cancellationToken: token);
        var error = await Assert.That(async () => await channel.QueueDeclarePassiveAsync(queue, token)).Throws<OperationInterruptedException>();
        return error!.ShutdownReason!.ReplyCode;
    }

    private sealed class NativeCredentials : ICredentialsProvider
    {
        private int _calls;
        public int Calls => Volatile.Read(ref _calls);
        public string Name => "native-test";
        public Task<Credentials> GetCredentialsAsync(CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _calls);
            return Task.FromResult(new Credentials(Name, "relayer", "relayer", null));
        }
    }

    // A virtual host per test on the session's shared broker. A test that stops the broker
    // to simulate an outage asks for a dedicated one instead.
    internal sealed class Broker : IAsyncDisposable
    {
        private readonly IContainer _container;
        private readonly bool _dedicated;
        internal string VirtualHost { get; }

        private Broker(IContainer container, string virtualHost, bool dedicated)
        {
            _container = container;
            VirtualHost = virtualHost;
            _dedicated = dedicated;
        }

        internal static async Task<Broker> StartAsync(CancellationToken token, bool delayedPlugin = false, bool dedicated = false)
        {
            if (dedicated) return new(await StartContainerAsync(delayedPlugin, shared: false, token), "/", true);
            var container = await (delayedPlugin ? SharedBrokers.RabbitMqWithDelayedPlugin : SharedBrokers.RabbitMq).Value.WaitAsync(token);
            var broker = new Broker(container, TestContext.Current!.Isolation.GetIsolatedName(Guid.NewGuid().ToString("N")[..8]), false);
            await broker.CommandAsync(["rabbitmqctl", "add_vhost", broker.VirtualHost], token);
            await broker.CommandAsync(["rabbitmqctl", "set_permissions", "-p", broker.VirtualHost, "relayer", ".*", ".*", ".*"], token);
            return broker;
        }

        internal static async Task<IContainer> StartContainerAsync(bool delayedPlugin, bool shared, CancellationToken token)
        {
            var builder = new ContainerBuilder("rabbitmq:4.1.4-alpine")
                .WithEnvironment("RABBITMQ_DEFAULT_USER", "relayer")
                .WithEnvironment("RABBITMQ_DEFAULT_PASS", "relayer")
                .WithPortBinding(5672, true)
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server startup complete"));
            // A dedicated broker serves one test; keep its Erlang VM small.
            if (!shared) builder = builder.WithEnvironment("RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS", "+S 2:2 +A 2");
            if (delayedPlugin)
            {
                using var http = new HttpClient();
                var plugin = await http.GetByteArrayAsync("https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/v4.1.0/rabbitmq_delayed_message_exchange-4.1.0.ez", token);
                builder = builder.WithResourceMapping(plugin, "/opt/rabbitmq/plugins/rabbitmq_delayed_message_exchange-4.1.0.ez")
                    .WithResourceMapping("[rabbitmq_delayed_message_exchange]."u8.ToArray(), "/etc/rabbitmq/enabled_plugins");
            }
            if (OperatingSystem.IsLinux()) builder = builder.WithPortBinding("20000-29999", "5672");
            var container = builder.Build();
            try { await container.StartAsync(token); return container; }
            catch { await container.DisposeAsync(); throw; }
        }

        internal ConnectionFactory Factory() => new()
        {
            HostName = _container.Hostname,
            Port = _container.GetMappedPublicPort(5672),
            UserName = "relayer",
            Password = "relayer",
            VirtualHost = VirtualHost,
        };

        internal async Task CommandAsync(string[] command, CancellationToken token)
        {
            var result = await _container.ExecAsync(command, token);
            if (result.ExitCode != 0) throw new InvalidOperationException(result.Stderr);
        }

        public async ValueTask DisposeAsync()
        {
            if (_dedicated)
            {
                await _container.DisposeAsync();
                return;
            }
            // Throwing here would replace an exception unwinding through await using; the session removes the broker.
            try { await CommandAsync(["rabbitmqctl", "delete_vhost", VirtualHost], CancellationToken.None); }
            catch (Exception exception) { Console.Error.WriteLine($"Could not delete RabbitMQ virtual host {VirtualHost}: {exception}"); }
        }
    }
}
