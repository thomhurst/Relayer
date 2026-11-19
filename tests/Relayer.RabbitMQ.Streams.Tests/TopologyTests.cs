using System.Net;
using System.Reflection;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RabbitMQ.Stream.Client;
using Relayer.Transports;

namespace Relayer.RabbitMQ.Streams.Tests;

public class TopologyTests
{
    [Test]
    public async Task Hosted_provision_is_idempotent_preserves_native_settings_and_owns_only_its_connection()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        await using var broker = await Broker.StartAsync(token);
        var configuration = broker.Configuration();
        await using var independent = await StreamSystem.Create(broker.Configuration());
        var spec = new StreamSpec("existing") { MaxLengthBytes = 123456789 };
        await independent.CreateStream(spec);
        var host = new HostBuilder().ConfigureServices(services => services.AddRelayer(builder => builder.RabbitMQStreams("streams", streams =>
        {
            streams.TopologyMode = TopologyMode.Provision;
            streams.Options.Configure(options =>
            {
                options.Configuration = configuration;
                options.Streams.Add(new("existing") { Configuration = spec });
                options.Streams.Add(new("created") { Configuration = new("created") });
            });
        }))).Build();
        await using var hostLifetime = (IAsyncDisposable)host;
        await host.StartAsync(token);
        await Assert.That(host.Services.GetRequiredService<IOptionsMonitor<RabbitMqStreamsOptions>>().Get("streams").Configuration).IsSameReferenceAs(configuration);
        var transport = host.Services.GetRequiredService<ITransport>();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);
        await Assert.That(await independent.StreamExists("created")).IsTrue();
        var arguments = await broker.CommandAsync(["rabbitmqctl", "list_queues", "name", "arguments", "--formatter=json"], token);
        await Assert.That(arguments).Contains("123456789");
        await using var drift = ConfigurationTests.Provider(options =>
        {
            options.Configuration = broker.Configuration();
            options.Streams.Add(new("existing") { Configuration = new("existing") { MaxLengthBytes = 999 } });
            options.Streams.Add(new("after-error") { Configuration = new("after-error") });
        });
        var report = await drift.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Provision, token);
        await Assert.That(report.Differences.Count).IsEqualTo(1);
        await Assert.That(report.Differences[0]).Contains("PreconditionFailed");
        await Assert.That(await independent.StreamExists("after-error")).IsTrue();
        await Assert.That(await broker.CommandAsync(["rabbitmqctl", "list_queues", "name", "arguments", "--formatter=json"], token)).Contains("123456789");
        await host.StopAsync(token);
        await transport.StopAsync(token);
        await transport.StartAsync(token);
        await transport.StopAsync(token);
        await Assert.That(await independent.StreamExists("existing")).IsTrue();
        await Assert.That(await independent.StreamExists("created")).IsTrue();
    }

    [Test]
    public async Task Validate_is_read_only_and_missing_stream_fails_host_startup()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        await using var broker = await Broker.StartAsync(token);
        await using var independent = await StreamSystem.Create(broker.Configuration());
        await independent.CreateStream(new StreamSpec("existing"));
        await broker.CommandAsync(["rabbitmqctl", "add_user", "reader", "reader"], token);
        await broker.CommandAsync(["rabbitmqctl", "set_permissions", "-p", "/", "reader", "^$", "^$", "^existing$"], token);
        await using var reader = ConfigurationTests.Provider(options =>
        {
            options.Configuration = broker.Configuration("reader");
            options.Streams.Add(new("existing"));
        });
        await Assert.That((await reader.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Validate, token)).Differences.Count).IsEqualTo(0);
        await using var nativeReader = await StreamSystem.Create(broker.Configuration("reader"));
        await Assert.That(async () => await nativeReader.CreateStream(new StreamSpec("forbidden"))).Throws<CreateStreamException>();
        await using var missing = ConfigurationTests.Provider(options =>
        {
            options.Configuration = broker.Configuration();
            options.Streams.Add(new("existing"));
            options.Streams.Add(new("missing"));
            options.Streams.Add(new("existing-settings") { Configuration = new("existing-settings") });
        });
        var differences = (await missing.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Validate, token)).Differences;
        await Assert.That(differences.Count).IsEqualTo(2);
        await Assert.That(differences[0]).Contains("missing");
        await Assert.That(await independent.StreamExists("missing")).IsFalse();
        await Assert.That((await missing.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(1);
        await Assert.That(await independent.StreamExists("missing")).IsFalse();
        await Assert.That(await independent.StreamExists("existing-settings")).IsTrue();
        await using var settings = ConfigurationTests.Provider(options =>
        {
            options.Configuration = broker.Configuration();
            options.Streams.Add(new("existing") { Configuration = new("existing") });
        });
        var unverified = (await settings.GetRequiredService<ITransport>().ApplyTopologyAsync(TopologyMode.Validate, token)).Differences;
        await Assert.That(unverified.Count).IsEqualTo(1);
        await Assert.That(unverified[0]).Contains("configuration equivalence checks are unavailable");
        var host = new HostBuilder().ConfigureServices(services => services.AddRelayer(builder => builder.RabbitMQStreams("streams", streams =>
            streams.Options.Configure(options =>
            {
                options.Configuration = broker.Configuration();
                options.Streams.Add(new("missing"));
            })))).Build();
        await using var hostLifetime = (IAsyncDisposable)host;
        await Assert.That(async () => await host.StartAsync(token)).Throws<OptionsValidationException>();
        await Assert.That(await independent.StreamExists("missing")).IsFalse();
    }

    [Test]
    public async Task Locator_recovers_natively_after_connection_is_closed()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var token = deadline.Token;
        await using var broker = await Broker.StartAsync(token);
        await using var provider = ConfigurationTests.Provider(options =>
        {
            options.Configuration = broker.Configuration();
            options.Streams.Add(new("recovery") { Configuration = new("recovery") });
        });
        var transport = provider.GetRequiredService<ITransport>();
        await transport.ApplyTopologyAsync(TopologyMode.Provision, token);
        // Inspect only the owned native instance in this test. Keeping this observation
        // test-local avoids adding a public API just to synchronize fault injection.
        var native = (StreamSystem)transport.GetType()
            .GetField("_system", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(transport)!;
        await Assert.That(native.IsClosed).IsFalse();
        // RabbitMQ's connection tracker includes stream protocol connections. Assert the
        // close actually targets our locator, rather than passing through a no-op fault.
        var closed = await broker.CommandAsync(["rabbitmqctl", "close_all_connections", "--global", "--per-connection-delay", "0", "native recovery test"], token);
        await Assert.That(closed).Contains("Closed 1 connections");
        // The broker command completing does not mean the client's socket reader has
        // observed the close. Native recovery checks IsClosed, and ServerProperties
        // otherwise returns cached data. Wait for that precondition without retrying RPCs.
        using var observation = new PeriodicTimer(TimeSpan.FromMilliseconds(10));
        while (!native.IsClosed) await observation.WaitForNextTickAsync(token);
        await transport.StartAsync(token);
        await Assert.That(native.IsClosed).IsFalse();
        await Assert.That((await transport.ApplyTopologyAsync(TopologyMode.Provision, token)).Differences.Count).IsEqualTo(0);
    }

    private sealed class Broker(IContainer container) : IAsyncDisposable
    {
        internal static async Task<Broker> StartAsync(CancellationToken token)
        {
            var builder = new ContainerBuilder("rabbitmq:4.1.4-alpine")
                .WithResourceMapping(Encoding.UTF8.GetBytes("[rabbitmq_stream].\n"), "/etc/rabbitmq/enabled_plugins")
                .WithEnvironment("RABBITMQ_DEFAULT_USER", "relayer")
                .WithEnvironment("RABBITMQ_DEFAULT_PASS", "relayer")
                .WithEnvironment("RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS", "+S 2:2 +A 2")
                .WithPortBinding(5552, true)
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server startup complete"));
            if (OperatingSystem.IsLinux()) builder = builder.WithPortBinding("20000-29999", "5552");
            var container = builder.Build();
            try { await container.StartAsync(token); return new(container); }
            catch { await container.DisposeAsync(); throw; }
        }

        internal StreamSystemConfig Configuration(string username = "relayer") => new()
        {
            UserName = username, Password = username,
            Endpoints = [new DnsEndPoint(container.Hostname, container.GetMappedPublicPort(5552))],
            RpcTimeOut = TimeSpan.FromSeconds(5),
        };

        internal async Task<string> CommandAsync(string[] command, CancellationToken token)
        {
            var result = await container.ExecAsync(command, token);
            if (result.ExitCode != 0) throw new InvalidOperationException(result.Stderr + result.Stdout);
            return result.Stdout;
        }

        public ValueTask DisposeAsync() => container.DisposeAsync();
    }
}
