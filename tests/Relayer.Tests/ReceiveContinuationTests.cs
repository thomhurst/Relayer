using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Tests;

public class ReceiveContinuationTests
{
    [Test]
    [Arguments(false, false, false)]
    [Arguments(true, false, false)]
    [Arguments(true, true, false)]
    [Arguments(false, false, true)]
    public async Task Execution_context_and_durable_order_survive_each_suspension(bool handlerFails, bool disposalFails, bool cancel)
    {
        var control = new Control();
        var services = new ServiceCollection().AddSingleton(control).AddScoped<Lifetime>();
        services.AddRelayer(r => { r.MessageTypes.Add<Payload>("continuation"); new ConsumerBuilder<object>(r.Services, r.Handlers, "manual").Handler<Handler>(); });
        await using var provider = services.BuildServiceProvider();
        var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), (context, _, _) =>
            {
                Check(control.Ambient.Value == "handler", "Failure lost handler execution context.");
                Check(!control.Disposed, "Scope disposed before durable failure handling.");
                control.Ambient.Value = "failure";
                control.FailureEntered.SetResult();
                return new(control.Failure.Task);
            })
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>())
            .Settle(context =>
            {
                Check(control.Disposed, "Settlement preceded async disposal.");
                Check(control.Ambient.Value == "dispose", "Settlement lost disposal execution context.");
                Check(!ServicesAvailable(context), "Services remained available during settlement.");
                control.Ambient.Value = "settlement";
                control.SettlementEntered.SetResult();
                return new(control.Settlement.Task);
            }).Build(1);
        var receive = new ReceivePipeline<Incoming, Binding>(pipeline, new HeaderNames(), 1);
        control.Ambient.Value = "caller";
        using var cancellation = new CancellationTokenSource();
        var pending = receive.ProcessAsync(new(), cancellation.Token);
        Check(!pending.IsCompleted, "Handler did not suspend.");
        Check(control.Ambient.Value == "caller", "Processing changed caller execution context.");
        if (cancel) cancellation.Cancel();
        control.BeforeDisposal = handlerFails ? "failure" : "handler";
        await Release(control, control.Handler, handlerFails ? new InvalidOperationException("handler") : null);
        if (handlerFails)
        {
            await control.FailureEntered.Task;
            Check(!control.DisposalEntered.Task.IsCompleted, "Disposal preceded durable failure completion.");
            await Release(control, control.Failure);
        }
        await control.DisposalEntered.Task;
        Check(!pending.IsCompleted, "Receive completed before disposal.");
        Check(control.Native!.Message is not null, "Native message cleared before disposal.");
        await Release(control, control.Disposal, disposalFails ? new ApplicationException("dispose") : null);
        if (disposalFails)
        {
            await Assert.That(async () => await pending).Throws<ApplicationException>().WithMessage("dispose");
            Check(!control.SettlementEntered.Task.IsCompleted, "Disposal failure permitted settlement.");
        }
        else if (cancel)
        {
            await Assert.That(async () => await pending).Throws<OperationCanceledException>();
            Check(!control.SettlementEntered.Task.IsCompleted, "Cancellation permitted settlement.");
        }
        else
        {
            await control.SettlementEntered.Task;
            Check(!pending.IsCompleted, "Receive completed before native settlement.");
            await Release(control, control.Settlement);
            await pending;
        }
        Check(control.Native!.Message is null, "Native message retained after completion.");
        Check(control.Ambient.Value == "caller", "Caller execution context changed after completion.");
    }

    [Test]
    public async Task Completed_unconsumed_receive_keeps_its_continuation_storage_reserved()
    {
        var control = new Control();
        var services = new ServiceCollection().AddSingleton(control).AddScoped<Lifetime>();
        services.AddRelayer(r => { r.MessageTypes.Add<Payload>("continuation"); new ConsumerBuilder<object>(r.Services, r.Handlers, "manual").Handler<Handler>(); });
        await using var provider = services.BuildServiceProvider();
        control.Disposal.SetResult();
        var consumer = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), static (_, error, _) => ValueTask.FromException(error))
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>()).Build(1);
        var receive = new ReceivePipeline<Incoming, Binding>(consumer, new HeaderNames(), 1);
        var first = receive.ProcessAsync(new());
        var firstNative = control.Native!;
        firstNative.Disposition = Disposition.Redeliver(TimeSpan.FromSeconds(1));
        control.Handler.SetResult();
        Check(first.IsCompleted, "Expected inline completion.");
        control.Handler = new();
        control.DisposalEntered = new();
        var second = receive.ProcessAsync(new());
        Check(!ReferenceEquals(firstNative, control.Native), "Unconsumed operation was reused.");
        control.Handler.SetResult();
        await Assert.That((await second).Kind).IsEqualTo(DispositionKind.Complete);
        await Assert.That((await first).Delay).IsEqualTo(TimeSpan.FromSeconds(1));
    }

    private static async Task Release(Control control, TaskCompletionSource gate, Exception? error = null)
    {
        await Task.Run(() =>
        {
            control.Ambient.Value = "worker";
            if (error is null) gate.SetResult();
            else gate.SetException(error);
            Check(control.Ambient.Value == "worker", "Continuation changed completing worker execution context.");
        });
    }
    private static bool ServicesAvailable(MessageContext context)
    {
        try { _ = context.Services; return true; }
        catch (InvalidOperationException) { return false; }
    }
    private static void Check(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }

    [Message("continuation")]
    public sealed class Payload;
    public sealed class Control
    {
        public AsyncLocal<string> Ambient { get; } = new();
        public TaskCompletionSource Handler { get; set; } = new();
        public TaskCompletionSource Failure { get; } = new();
        public TaskCompletionSource Disposal { get; } = new();
        public TaskCompletionSource Settlement { get; } = new();
        public TaskCompletionSource FailureEntered { get; } = new();
        public TaskCompletionSource DisposalEntered { get; set; } = new();
        public TaskCompletionSource SettlementEntered { get; } = new();
        public string? BeforeDisposal { get; set; }
        public bool Disposed { get; set; }
        internal NativeMessage<Incoming>? Native { get; set; }
    }
    public sealed class Handler(Control control) : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken)
        {
            _ = context.Services.GetRequiredService<Lifetime>();
            control.Native = context.Features.GetRequired<NativeMessage<Incoming>>();
            control.Ambient.Value = "handler";
            return new(control.Handler.Task);
        }
    }
    private sealed class Lifetime(Control control) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            if (control.BeforeDisposal is { } expected)
                Check(control.Ambient.Value == expected, "Disposal lost execution context.");
            control.Ambient.Value = "dispose";
            control.Disposed = true;
            control.DisposalEntered.SetResult();
            return new(control.Disposal.Task);
        }
    }
    private sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "test/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)_payload;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }
    internal sealed class Incoming;
    private readonly struct Binding : IMessageBinding<Incoming>
    {
        public static ReadOnlySequence<byte> GetBody(Incoming message) => ReadOnlySequence<byte>.Empty;
        public static int? GetDeliveryCount(Incoming message) => 1;
        public static bool TryGetHeader(Incoming message, string name, out WireString value)
        {
            value = name switch { "ce_id" => new("id"), "ce_type" => new("continuation;v=1"), _ => default };
            return !value.IsEmpty;
        }
    }
}
