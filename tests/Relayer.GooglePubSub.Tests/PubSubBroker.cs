using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Google.Cloud.PubSub.V1;
using Grpc.Net.Client;

namespace Relayer.GooglePubSub.Tests;

internal sealed class PubSubBroker(IContainer container, IContainer proxy, INetwork network, HttpClient control, GrpcChannel channel) : IAsyncDisposable
{
    internal const string Image = "gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:112c21f8f6e04b03b393b3c34fb943a1f443e839f18c6492c364ac2e6c3ff935";
    internal const string ProxyImage = "ghcr.io/shopify/toxiproxy@sha256:5d3dae09bc185dcc5ff6cf43abf6062a4a720aff0c4306a471e773acdcd107aa";
    internal PublisherServiceApiClient Publisher { get; } = new PublisherServiceApiClientImpl(new Publisher.PublisherClient(channel), null, null);
    internal SubscriberServiceApiClient Subscriber { get; } = new SubscriberServiceApiClientImpl(new Subscriber.SubscriberClient(channel), null, null);
    internal string Address => $"{proxy.Hostname}:{proxy.GetMappedPublicPort(8085)}";

    internal static async Task<PubSubBroker> StartAsync(CancellationToken token)
    {
        var network = new NetworkBuilder().Build();
        var container = new ContainerBuilder(Image).WithNetwork(network).WithNetworkAliases("pubsub-broker")
            .WithCommand("gcloud", "beta", "emulators", "pubsub", "start", "--project=relayer-test", "--host-port=0.0.0.0:8085", "--quiet")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server started, listening on 8085"))
            .Build();
        var proxy = new ContainerBuilder(ProxyImage).WithNetwork(network).WithPortBinding(8085, true).WithPortBinding(8474, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request => request.ForPort(8474).ForPath("/version"))).Build();
        HttpClient? control = null;
        try
        {
            await network.CreateAsync(token);
            await Task.WhenAll(container.StartAsync(token), proxy.StartAsync(token));
            control = new() { BaseAddress = new($"http://{proxy.Hostname}:{proxy.GetMappedPublicPort(8474)}") };
            using var body = new StringContent("{\"name\":\"pubsub\",\"listen\":\"0.0.0.0:8085\",\"upstream\":\"pubsub-broker:8085\"}", System.Text.Encoding.UTF8, "application/json");
            using var response = await control.PostAsync("/proxies", body, token);
            response.EnsureSuccessStatusCode();
            return new(container, proxy, network, control, GrpcChannel.ForAddress($"http://{proxy.Hostname}:{proxy.GetMappedPublicPort(8085)}"));
        }
        catch
        {
            control?.Dispose();
            try { await proxy.DisposeAsync(); }
            finally
            {
                try { await container.DisposeAsync(); }
                finally { await network.DisposeAsync(); }
            }
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
        try { await proxy.DisposeAsync(); }
        finally
        {
            try { await container.DisposeAsync(); }
            finally { await network.DisposeAsync(); }
        }
    }
}
