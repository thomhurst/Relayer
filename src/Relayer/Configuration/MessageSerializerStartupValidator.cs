using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Relayer.Serialization;

namespace Relayer;

internal sealed class MessageSerializerStartupValidator(IServiceProvider services) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Resolve the final registration so a custom serializer never needs JSON metadata.
        _ = services.GetRequiredService<IMessageSerializer>();
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
