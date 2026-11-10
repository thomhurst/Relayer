using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Google.Cloud.PubSub.V1;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;

namespace Relayer.GooglePubSub.Tests;

// One proxy per test in front of the session's shared emulator, so a test can cut its own
// connection without affecting the others. Tests name their topics and subscriptions uniquely.
internal sealed class PubSubBroker(IContainer proxy, HttpClient control, GrpcChannel channel) : IAsyncDisposable
{
    internal const string Image = "gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:112c21f8f6e04b03b393b3c34fb943a1f443e839f18c6492c364ac2e6c3ff935";
    internal const string ProxyImage = "ghcr.io/shopify/toxiproxy@sha256:5d3dae09bc185dcc5ff6cf43abf6062a4a720aff0c4306a471e773acdcd107aa";
    internal PublisherServiceApiClient Publisher { get; } = new PublisherServiceApiClientImpl(new Publisher.PublisherClient(channel), null, null);
    internal SubscriberServiceApiClient Subscriber { get; } = new SubscriberServiceApiClientImpl(new Subscriber.SubscriberClient(channel), null, null);
    internal Grpc.Core.CallInvoker Invoker => channel.CreateCallInvoker();
    internal string Address => $"{proxy.Hostname}:{proxy.GetMappedPublicPort(8085)}";

    internal static async Task<PubSubBroker> StartAsync(CancellationToken token, ILoggerFactory? loggerFactory = null)
    {
        var emulator = await PubSubSession.Current.WaitAsync(token);
        var proxy = new ContainerBuilder(ProxyImage).WithNetwork(emulator.Network).WithPortBinding(8085, true).WithPortBinding(8474, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request => request.ForPort(8474).ForPath("/version"))).Build();
        HttpClient? control = null;
        try
        {
            await proxy.StartAsync(token);
            control = new() { BaseAddress = new($"http://{proxy.Hostname}:{proxy.GetMappedPublicPort(8474)}") };
            using var body = new StringContent("{\"name\":\"pubsub\",\"listen\":\"0.0.0.0:8085\",\"upstream\":\"pubsub-broker:8085\"}", System.Text.Encoding.UTF8, "application/json");
            using var response = await control.PostAsync("/proxies", body, token);
            response.EnsureSuccessStatusCode();
            return new(proxy, control, GrpcChannel.ForAddress($"http://{proxy.Hostname}:{proxy.GetMappedPublicPort(8085)}", new GrpcChannelOptions { LoggerFactory = loggerFactory }));
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
        using var response = await control.PostAsync("/proxies/pubsub", body, token);
        response.EnsureSuccessStatusCode();
    }
    public async ValueTask DisposeAsync()
    {
        channel.Dispose();
        control.Dispose();
        await proxy.DisposeAsync();
    }
}

public static class PubSubSession
{
    // Started by the first test that needs the emulator, so filtered runs of pure unit tests start no container.
    private static Lazy<Task<Emulator>> _shared = new(StartAsync);

    internal static Task<Emulator> Current => Volatile.Read(ref _shared).Value;

    internal sealed record Emulator(IContainer Container, INetwork Network);

    private static async Task<Emulator> StartAsync()
    {
        using var provisioning = new CancellationTokenSource(TimeSpan.FromMinutes(10));
        var network = new NetworkBuilder().Build();
        var container = new ContainerBuilder(PubSubBroker.Image).WithNetwork(network).WithNetworkAliases("pubsub-broker")
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
            try { await container.DisposeAsync(); }
            finally { await network.DisposeAsync(); }
            throw;
        }
    }

    [After(TestSession)]
    public static async Task StopAsync()
    {
        // Like TUnit's own per-session instances, a later session in this process starts afresh.
        var shared = Interlocked.Exchange(ref _shared, new(StartAsync));
        if (!shared.IsValueCreated) return;
        // Wait for a startup still in flight (every test may have given up on it) so its containers are not left behind.
        Emulator emulator;
        try { emulator = await shared.Value; }
        // A failed startup already cleaned up and failed the tests that needed it.
        catch { return; }
        try { await emulator.Container.DisposeAsync(); }
        finally { await emulator.Network.DisposeAsync(); }
    }
}
