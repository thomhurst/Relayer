using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Relayer.Transports;

namespace Relayer.Nats.Tests;

public class NatsTopologyTests
{
    [Test]
    public async Task Hosted_provision_is_idempotent_native_and_non_destructive()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        await using var broker = await Broker.StartAsync(token);
        var nativeOptions = broker.Options();
        var stream = new StreamConfig("orders", ["orders.>"]) { Storage = StreamConfigStorage.Memory, MaxMsgs = 100 };
        var consumer = new ConsumerConfig("handler") { FilterSubject = "orders.placed", AckWait = TimeSpan.FromSeconds(20) };
        var host = new HostBuilder().ConfigureServices(services => services.AddRelayer(builder => builder.Nats("nats", nats =>
        {
            nats.TopologyMode = TopologyMode.Provision;
            nats.Options.Configure(options =>
            {
                options.ConnectionOptions = nativeOptions;
                options.Streams.Add(new("orders") { Configuration = stream });
                options.Consumers.Add(new("orders", "handler") { Configuration = consumer });
            });
        }))).Build();
        await using var hostLifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        var transport = host.Services.GetRequiredService<ITransport>();
        await Assert.That(host.Services.GetRequiredService<IOptionsMonitor<NatsTransportOptions>>().Get("nats").ConnectionOptions).IsSameReferenceAs(nativeOptions);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);

        await using var independent = new NatsConnection(nativeOptions);
        var js = new NatsJSContext(independent);
        var actual = await js.GetStreamAsync("orders", cancellationToken: token);
        await Assert.That(actual.Info.Config.Storage).IsEqualTo(StreamConfigStorage.Memory);
        await Assert.That(actual.Info.Config.MaxMsgs).IsEqualTo(100L);
        var handler = await js.GetConsumerAsync("orders", "handler", token);
        await Assert.That(handler.Info.Config.AckWait).IsEqualTo(TimeSpan.FromSeconds(20));
        await Assert.That(handler.Info.Config.DurableName).IsEqualTo("handler");

        await using var drift = NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = broker.Options();
            options.Streams.Add(new("orders") { Configuration = stream with { Storage = StreamConfigStorage.File } });
            options.Consumers.Add(new("orders", "handler") { Configuration = consumer with { AckWait = TimeSpan.FromSeconds(5) } });
            options.Streams.Add(new("after_error") { Configuration = new("after_error", ["after.error"]) });
        });
        var report = await drift.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Provision, token);
        await Assert.That(report.Differences.Count).IsEqualTo(2);
        await Assert.That(report.Differences[0]).Contains("orders");
        await Assert.That(report.Differences[1]).Contains("handler");
        await js.GetStreamAsync("after_error", cancellationToken: token);
        await Assert.That((await js.GetStreamAsync("orders", cancellationToken: token)).Info.Config.Storage).IsEqualTo(StreamConfigStorage.Memory);
        await Assert.That((await js.GetConsumerAsync("orders", "handler", token)).Info.Config.AckWait).IsEqualTo(TimeSpan.FromSeconds(20));
        await Assert.That(stream.Storage).IsEqualTo(StreamConfigStorage.Memory);
        await Assert.That(consumer.AckWait).IsEqualTo(TimeSpan.FromSeconds(20));

        await host.StopAsync(token);
        await transport.StopAsync(token);
        await independent.PingAsync(token);
        await transport.StartAsync(token);
        await transport.StopAsync(token);
        await js.GetStreamAsync("orders", cancellationToken: token);
    }

    [Test]
    public async Task Validate_reports_missing_and_unverified_configuration_without_creating_entities()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        await using var broker = await Broker.StartAsync(token);
        await using var connection = new NatsConnection(broker.Options());
        var js = new NatsJSContext(connection);
        await js.CreateStreamAsync(new("events", ["events.>"]), token);
        await js.CreateConsumerAsync("events", new("handler"), token);
        await using var names = NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = broker.Options();
            options.Streams.Add(new("events"));
            options.Consumers.Add(new("events", "handler"));
        });
        var transport = names.GetRequiredService<ITransport>();
        for (var attempt = 0; attempt < 2; attempt++)
            await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);

        await using var missing = NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = broker.Options();
            options.Streams.Add(new("missing"));
            options.Consumers.Add(new("events", "missing"));
            options.Streams.Add(new("events") { Configuration = new("events", ["events.>"]) });
        });
        var differences = (await missing.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Validate, token)).Differences;
        await Assert.That(differences.Count).IsEqualTo(3);
        await Assert.That(differences[0]).Contains("missing");
        await Assert.That(differences[1]).Contains("equivalence checks are not implemented");
        await Assert.That(differences[2]).Contains("missing");
        await Assert.That(async () => await js.GetStreamAsync("missing", cancellationToken: token)).Throws<NatsJSApiException>();
        await Assert.That(async () => await js.GetConsumerAsync("events", "missing", token)).Throws<NatsJSApiException>();
        await Assert.That((await js.GetStreamAsync("events", cancellationToken: token)).Info.State.Messages).IsEqualTo(0L);

        var host = new HostBuilder().ConfigureServices(services => services.AddRelayer(builder => builder.Nats("nats", nats =>
            nats.Options.Configure(options =>
            {
                options.ConnectionOptions = broker.Options();
                options.Streams.Add(new("missing"));
            })))).Build();
        await using var hostLifetime = (IAsyncDisposable)host;
        var error = await Assert.That(async () => await host.StartAsync(token)).Throws<OptionsValidationException>();
        await Assert.That(error!.Message).Contains("missing");
    }

    [Test]
    public async Task Existence_validation_uses_read_only_permissions_and_native_recovery_after_broker_restart()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        await using var broker = await Broker.StartAsync(token, """
            authorization {
                users: [
                    { user: admin, password: admin },
                    { user: reader, password: reader, permissions: {
                        publish: ["$JS.API.STREAM.INFO.>", "$JS.API.CONSUMER.INFO.>"],
                        subscribe: ["_INBOX.>"]
                    }}
                ]
            }
            """);
        await using var admin = new NatsConnection(broker.Options("admin"));
        var js = new NatsJSContext(admin);
        await js.CreateStreamAsync(new("events", ["events.>"]), token);
        await js.CreateConsumerAsync("events", new("handler"), token);
        await using var provider = NatsConfigurationTests.Provider(options =>
        {
            options.ConnectionOptions = broker.Options("reader");
            options.Streams.Add(new("events"));
            options.Consumers.Add(new("events", "handler"));
        });
        var transport = provider.GetRequiredService<ITransport>();
        await transport.StartAsync(token);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);
        var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reopened = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        admin.ConnectionDisconnected += (_, _) => { disconnected.TrySetResult(); return default; };
        admin.ConnectionOpened += (_, _) =>
        {
            if (disconnected.Task.IsCompleted) reopened.TrySetResult();
            return default;
        };
        await broker.RestartAsync(token);
        await reopened.Task.WaitAsync(token);
        // Native ping waits for the client's reconnect. No adapter reconnect loop or timing sleep is needed.
        await transport.StartAsync(token);
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);
        await Assert.That((await js.GetStreamAsync("events", cancellationToken: token)).Info.State.Messages).IsEqualTo(0L);
    }

    private sealed class Broker(IContainer container) : IAsyncDisposable
    {
        internal static async Task<Broker> StartAsync(CancellationToken token, string? configuration = null)
        {
            var builder = new ContainerBuilder("nats:2.15.2-alpine")
                .WithPortBinding(4222, true)
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server is ready"));
            if (configuration is not null)
                builder = builder.WithResourceMapping(Encoding.UTF8.GetBytes(configuration), "/etc/nats.conf");
            var arguments = configuration is null ? "-js" : "-js -c /etc/nats.conf";
            // Docker stop/start reallocates random published ports. Restart only the broker process
            // so the recovery test preserves the endpoint every existing native client uses.
            builder = builder.WithEntrypoint("/bin/sh", "-c").WithCommand($"""
                while true; do
                    nats-server {arguments} &
                    broker=$!
                    trap 'kill -TERM "$broker"; wait "$broker"; exit 0' TERM INT
                    wait "$broker"
                done
                """);
            if (OperatingSystem.IsLinux()) builder = builder.WithPortBinding("20000-29999", "4222");
            var container = builder.Build();
            try { await container.StartAsync(token); return new(container); }
            catch { await container.DisposeAsync(); throw; }
        }

        internal NatsOpts Options(string? username = null) => new()
        {
            Url = $"nats://{container.Hostname}:{container.GetMappedPublicPort(4222)}",
            ConnectTimeout = TimeSpan.FromSeconds(5),
            RequestTimeout = TimeSpan.FromSeconds(5),
            AuthOpts = username is null ? new() : new() { Username = username, Password = username },
        };

        internal async Task RestartAsync(CancellationToken token)
        {
            var result = await container.ExecAsync(["killall", "-TERM", "nats-server"], token);
            if (result.ExitCode != 0) throw new InvalidOperationException(result.Stderr);
        }

        public ValueTask DisposeAsync() => container.DisposeAsync();
    }
}
