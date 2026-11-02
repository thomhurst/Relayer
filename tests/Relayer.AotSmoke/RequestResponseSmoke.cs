using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer;
using Relayer.InMemory;

internal static class RequestResponseSmoke
{
    internal static async Task RunAsync()
    {
        var services = new ServiceCollection();
        services.AddRelayer(builder =>
        {
            builder.AddMessageTypesFromThisAssembly();
            builder.InMemory("rpc", transport =>
            {
                transport.Request<SmokeRequest, SmokeResponse>("requests", "replies");
                transport.Send<SmokeResponse>("replies");
                transport.Consume("requests", consumer => consumer.Handler<SmokeRequestHandler>());
            });
        });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var hosted = provider.GetServices<IHostedService>().ToArray();
        try
        {
            foreach (var lifecycle in hosted.OfType<IHostedLifecycleService>()) await lifecycle.StartingAsync(default);
            foreach (var service in hosted) await service.StartAsync(default);
            using var scope = provider.CreateScope();
            var client = scope.ServiceProvider.GetRequiredService<IRequestClient<SmokeRequest, SmokeResponse>>();
            var response = await client.RequestAsync(new(42), TimeSpan.FromSeconds(10));
            if (response.Value != 42) throw new InvalidOperationException("Request/response correlation failed under Native AOT.");
        }
        finally
        {
            foreach (var service in hosted.Reverse()) await service.StopAsync(default);
        }
        Console.WriteLine("Request/response passed under Native AOT.");
    }
}

[Message("smoke.request")]
public sealed record SmokeRequest(int Value);
[Message("smoke.response")]
public sealed record SmokeResponse(int Value);
public sealed class SmokeRequestHandler : IHandler<SmokeRequest>
{
    public ValueTask HandleAsync(SmokeRequest message, MessageContext context, CancellationToken cancellationToken) =>
        context.ReplyAsync(new SmokeResponse(message.Value), cancellationToken);
}
