using System.Runtime.CompilerServices;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace Relayer.Nats;

internal sealed class NatsConsumerWorker(string stream, string name, NatsJSConsumeOpts settings, int concurrency, MessageTracing tracing)
{
    private readonly MessageOperationTracing _ack = new(tracing, new() { System = "nats", Destination = stream, Subscription = name }, MessagingOperation.Settle, "ack");
    private CancellationTokenSource? _pull;
    private CancellationTokenSource? _processing;
    internal Func<NatsDelivery, CancellationToken, ValueTask<Disposition>> Receive { get; set; } = null!;
    internal Task Completion { get; private set; } = Task.CompletedTask;
    internal async Task StartAsync(NatsJSContext context, CancellationToken token)
    {
        if (_pull is not null) throw new InvalidOperationException("JetStream consumer is already running.");
        var consumer = await context.GetConsumerAsync(stream, name, token).ConfigureAwait(false);
        Validate(consumer.Info.Config, stream, name);
        _pull = new();
        _processing = new();
        var tasks = new Task[concurrency];
        if (ExecutionContext.IsFlowSuppressed())
            for (var i = 0; i < tasks.Length; i++) tasks[i] = RunAsync(consumer, _pull.Token, _processing.Token);
        else
        {
            using (ExecutionContext.SuppressFlow())
                for (var i = 0; i < tasks.Length; i++) tasks[i] = RunAsync(consumer, _pull.Token, _processing.Token);
        }
        Completion = Task.WhenAll(tasks);
    }
    internal static void Validate(ConsumerConfig config, string stream, string name)
    {
        if (config.AckPolicy != ConsumerConfigAckPolicy.Explicit || config.DeliverSubject is not null ||
            config.DurableName != name || config.HeadersOnly || config.MaxDeliver > 0)
            throw new InvalidOperationException($"NATS consumer '{stream}/{name}': use a durable pull consumer with AckPolicy.Explicit, full payloads and unlimited MaxDeliver. Finite MaxDeliver can strand poison messages before the durable dead-letter copy succeeds.");
    }
    private async Task RunAsync(INatsJSConsumer consumer, CancellationToken pull, CancellationToken processing)
    {
        try
        {
            await foreach (var message in consumer.ConsumeAsync(NatsRawSerializer<ReadOnlyMemory<byte>>.Default, settings, pull).ConfigureAwait(false))
            {
                message.EnsureSuccess();
                var delivery = new NatsDelivery(message, consumer);
                var disposition = await Receive(delivery, processing).ConfigureAwait(false);
                if (disposition.Kind != DispositionKind.SettledByHandler)
                    await SettleAsync(delivery, disposition, processing).ConfigureAwait(false);
            }
        }
        catch
        {
            // A failed native enumerator must stop its siblings so the hosted completion can fail.
            await _processing!.CancelAsync().ConfigureAwait(false);
            await _pull!.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask SettleContextAsync(MessageContext context)
    {
        var feature = context.Features.GetRequired<NativeMessage<NatsDelivery>>();
        await SettleAsync(feature.Message, feature.Disposition, context.CancellationToken).ConfigureAwait(false);
        feature.Disposition = Disposition.SettledByHandler;
    }
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal async ValueTask SettleAsync(NatsDelivery delivery, Disposition disposition, CancellationToken token)
    {
        if (disposition.Kind == DispositionKind.SettledByHandler) return;
        if (disposition.Kind != DispositionKind.Complete)
            throw new NotSupportedException("NATS settlement uses confirmed ACK after durable work. NAK cannot update failure headers; use in-process retries and a DeadLetterEndpoint, or native message APIs explicitly.");
        using var activity = _ack.Start();
        await delivery.Message.AckAsync(new() { DoubleAck = true }, token).ConfigureAwait(false);
    }
    internal async Task StopAsync(CancellationToken token)
    {
        if (_pull is null) return;
        using var abort = token.Register(static state => ((CancellationTokenSource)state!).Cancel(), _processing);
        await _pull.CancelAsync().ConfigureAwait(false);
        // Native DrainOnCancel stops pulls, fences buffered deliveries and leaves the connection open for ACKs.
        try { await Completion.WaitAsync(token).ConfigureAwait(false); }
        finally
        {
            if (Completion.IsCompleted)
            {
                _pull.Dispose();
                _processing!.Dispose();
                _pull = null;
                _processing = null;
            }
        }
    }
}
