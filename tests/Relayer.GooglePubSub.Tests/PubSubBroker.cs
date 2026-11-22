using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Google.Cloud.PubSub.V1;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;

namespace Relayer.GooglePubSub.Tests;

// Outage tests get their own proxy so they can cut a connection without affecting
// other tests. The rest connect directly to the session's shared emulator.
internal sealed class PubSubBroker(GrpcChannel channel, IContainer? proxy = null, HttpClient? control = null) : IAsyncDisposable
{
    internal const string Image = "gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:112c21f8f6e04b03b393b3c34fb943a1f443e839f18c6492c364ac2e6c3ff935";
    internal const string ProxyImage = "ghcr.io/shopify/toxiproxy@sha256:5d3dae09bc185dcc5ff6cf43abf6062a4a720aff0c4306a471e773acdcd107aa";

    internal PublisherServiceApiClient Publisher
    {
        get;
    } = new PublisherServiceApiClientImpl(new Publisher.PublisherClient(channel), null, null);

    internal SubscriberServiceApiClient Subscriber
    {
        get;
    } = new SubscriberServiceApiClientImpl(new Subscriber.SubscriberClient(channel), null, null);

    internal Grpc.Core.CallInvoker Invoker => channel.CreateCallInvoker();

    internal static async Task<PubSubBroker> StartAsync(CancellationToken token, bool useProxy = false, ILoggerFactory? loggerFactory = null)
    {
        var emulator = await PubSubSession.Current.WaitAsync(token);
        var channelOptions = new GrpcChannelOptions { LoggerFactory = loggerFactory };

        if (!useProxy)
        {
            return new(GrpcChannel.ForAddress($"http://{emulator.Container.Hostname}:{emulator.Container.GetMappedPublicPort(8085)}", channelOptions));
        }

        var proxy = new ContainerBuilder(ProxyImage).WithNetwork(emulator.Network).WithPortBinding(8085, true).WithPortBinding(8474, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request => request.ForPort(8474).ForPath("/version"))).Build();
        HttpClient? control = null;

        try
        {
            await proxy.StartAsync(token);
            control = new()
            {
                BaseAddress = new($"http://{proxy.Hostname}:{proxy.GetMappedPublicPort(8474)}")
            };
            using var body = new StringContent("{\"name\":\"pubsub\",\"listen\":\"0.0.0.0:8085\",\"upstream\":\"pubsub-broker:8085\"}", System.Text.Encoding.UTF8, "application/json");
            using var response = await control.PostAsync("/proxies", body, token);
            response.EnsureSuccessStatusCode();
            var channel = GrpcChannel.ForAddress($"http://{proxy.Hostname}:{proxy.GetMappedPublicPort(8085)}", channelOptions);
            return new(channel, proxy, control);
        }
        catch
        {
            control?.Dispose();
            await proxy.DisposeAsync();
            throw;
        }
    }

    internal async Task SetConnectedAsync(bool connected, CancellationToken token)
    {
        using var body = new StringContent(connected ? "{\"enabled\":true}" : "{\"enabled\":false}", System.Text.Encoding.UTF8, "application/json");
        var client = control ?? throw new InvalidOperationException("Disconnect and reconnect tests require a proxy.");
        using var response = await client.PostAsync("/proxies/pubsub", body, token);
        response.EnsureSuccessStatusCode();

        if (connected)
        {
            // The listener can reopen while the channel still reports the outage's connection failure.
            await channel.ConnectAsync(token);
        }
    }

    public async ValueTask DisposeAsync()
    {
        channel.Dispose();
        control?.Dispose();

        if (proxy is not null)
        {
            await proxy.DisposeAsync();
        }
    }
}

public static class PubSubSession
{
    // Started by the first test that needs the emulator, so filtered runs of pure unit tests start no container.
    private static Lazy<Task<Emulator>> s_shared = new(StartAsync);

    internal static Task<Emulator> Current => Volatile.Read(ref s_shared).Value;

    internal sealed record Emulator(IContainer Container, INetwork Network);

    private static async Task<Emulator> StartAsync()
    {
        using var provisioning = new CancellationTokenSource(TimeSpan.FromMinutes(10));
        var network = new NetworkBuilder().Build();
        var container = new ContainerBuilder(PubSubBroker.Image).WithNetwork(network).WithNetworkAliases("pubsub-broker").WithPortBinding(8085, true)
            .WithCommand("gcloud", "beta", "emulators", "pubsub", "start", "--project=relayer-test", "--host-port=0.0.0.0:8085", "--quiet")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server started, listening on 8085"))
            .Build();

        try
        {
            await network.CreateAsync(provisioning.Token);
            await container.StartAsync(provisioning.Token);
            return new(container, network);
        }
        catch
        {
            try
            {
                await container.DisposeAsync();
            }
            finally
            {
                await network.DisposeAsync();
            }

            throw;
        }
    }

    [After(TestSession)]
    public static async Task StopAsync()
    {
        // Like TUnit's own per-session instances, a later session in this process starts afresh.
        var shared = Interlocked.Exchange(ref s_shared, new(StartAsync));

        if (!shared.IsValueCreated)
        {
            return;
        }
        // Wait for a startup still in flight (every test may have given up on it) so its containers are not left behind.
        Emulator emulator;

        try
        {
            emulator = await shared.Value;
        }
        // A failed startup already cleaned up and failed the tests that needed it.
        catch
        {
            return;
        }

        try
        {
            await emulator.Container.DisposeAsync();
        }
        finally
        {
            await emulator.Network.DisposeAsync();
        }
    }
}
