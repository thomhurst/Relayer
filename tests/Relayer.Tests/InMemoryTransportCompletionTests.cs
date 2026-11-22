using Relayer.InMemory;
using Relayer.Transports;

namespace Relayer.Tests;

public class InMemoryTransportCompletionTests
{
    [Test]
    [Arguments(ConsumptionModel.Queue)]
    [Arguments(ConsumptionModel.Log)]
    public async Task Callback_cancellation_stops_transport_and_surfaces_original_exception(ConsumptionModel model)
    {
        await using var transport = new InMemoryTransport("local", new Dictionary<string, InMemoryEndpointOptions>
        {
            ["orders"] = new()
            {
                Capabilities = new()
                {
                    ConsumptionModel = model,
                },
            },
        });
        var limit = TimeSpan.FromSeconds(5);
        var error = new OperationCanceledException("Receive callback canceled", new CancellationToken(true));
        transport.RegisterConsumer("orders", (_, _) => throw error, group: model == ConsumptionModel.Log ? "workers" : null);

        await transport.StartAsync(default);
        await transport.SendAsync(new("orders", new byte[] { 1 }), default);

        var completionError = await Assert.That(async () => await transport.Completion.WaitAsync(limit))
            .Throws<OperationCanceledException>();
        await Assert.That(completionError).IsSameReferenceAs(error);
        await Assert.That(transport.Completion.IsCanceled).IsTrue();

        var stopError = await Assert.That(async () => await transport.StopAsync(default).WaitAsync(limit))
            .Throws<OperationCanceledException>();
        await Assert.That(stopError).IsSameReferenceAs(error);
    }
}
