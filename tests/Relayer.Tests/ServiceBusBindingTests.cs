using System.Buffers;
using Azure.Core.Amqp;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Relayer.AzureServiceBus;
using Relayer.Serialization;
using Relayer.Transports;

namespace Relayer.Tests;

public partial class ServiceBusBindingTests
{
    [Test]
    public async Task Binding_preserves_sdk_multi_section_body_conversion()
    {
        var message = Args(new RecordingReceiver()).Message;
        message.GetRawAmqpMessage().Body = AmqpMessageBody.FromData(new ReadOnlyMemory<byte>[] { new byte[] { 1, 2 }, new byte[] { 3 } });
        var args = new ProcessMessageEventArgs(message, new RecordingReceiver(), "multi-section", default);
        await Assert.That(ServiceBusBinding.GetBody(args).ToArray().AsSpan().SequenceEqual(message.Body.ToMemory().Span)).IsTrue();
    }

    [Test]
    public async Task Binding_borrows_native_body_and_reads_only_requested_text_or_binary_headers()
    {
        var body = new BinaryData(new byte[] { 1, 2, 3 });
        var binary = new byte[] { 0xE6, 0x9D, 0xB1 };
        var text = new string('x', 11);
        var message = ServiceBusModelFactory.ServiceBusReceivedMessage(body: body, deliveryCount: 7,
            properties: new Dictionary<string, object>
            {
                ["cloudEvents:type"] = text, ["binary"] = binary, ["memory"] = (ReadOnlyMemory<byte>)binary,
                ["segment"] = new ArraySegment<byte>(binary), ["number"] = 42,
            });
        var args = new ProcessMessageEventArgs(message, new RecordingReceiver(), "test", default);

        await Assert.That(ServiceBusBinding.GetBody(args).First.Equals(body.ToMemory())).IsTrue();
        await Assert.That(ServiceBusBinding.GetDeliveryCount(args)).IsEqualTo(7);
        await Assert.That(ServiceBusBinding.TryGetHeader(args, "cloudEvents:type", out var value)).IsTrue();
        await Assert.That(value.Text).IsSameReferenceAs(text);
        foreach (var name in new[] { "binary", "memory", "segment" })
        {
            await Assert.That(ServiceBusBinding.TryGetHeader(args, name, out value)).IsTrue();
            await Assert.That(value.Text).IsNull();
            await Assert.That(value.Utf8.Equals((ReadOnlyMemory<byte>)binary)).IsTrue();
        }
        await Assert.That(ServiceBusBinding.TryGetHeader(args, "missing", out value)).IsFalse();
        await Assert.That(value.IsEmpty).IsTrue();
        await Assert.That(ServiceBusBinding.TryGetHeader(args, "number", out _)).IsFalse();
        await Assert.That(message.ApplicationProperties["number"]).IsEqualTo(42);
    }

    [Test]
    [Arguments("application/cbor", null)]
    [Arguments("application/cbor", "application/json")]
    [Arguments("", "application/json")]
    [Arguments(null, "application/json")]
    [Arguments(null, null)]
    public async Task Binding_reads_native_content_type_before_application_property(string? native, string? fallback)
    {
        var properties = new Dictionary<string, object> { ["other"] = "unchanged" };
        if (fallback is not null) properties["content-type"] = fallback;
        var message = ServiceBusModelFactory.ServiceBusReceivedMessage(contentType: native, properties: properties);
        var args = new ProcessMessageEventArgs(message, new RecordingReceiver(), "content-type", default);

        var found = ServiceBusBinding.TryGetHeader(args, "content-type", out var value);
        var expected = native ?? fallback;
        await Assert.That(found).IsEqualTo(expected is not null);
        await Assert.That(value.Text).IsSameReferenceAs(expected);
        await Assert.That(ServiceBusBinding.TryGetHeader(args, "other", out value)).IsTrue();
        await Assert.That(value.Text).IsEqualTo("unchanged");
        await Assert.That(message.ContentType).IsEqualTo(native);
        await Assert.That(message.ApplicationProperties.ContainsKey("content-type")).IsEqualTo(fallback is not null);
    }

    [Test]
    public async Task Native_content_type_is_visible_through_context_headers()
    {
        var args = Args(new RecordingReceiver());
        args.Message.GetRawAmqpMessage().Properties.ContentType = "application/cbor";
        await using var provider = Provider();
        var handled = false;
        provider.GetRequiredService<Control>().Handle = async context =>
        {
            await Assert.That(context.Headers.TryGetValue("content-type", out var value)).IsTrue();
            await Assert.That(value.Text).IsEqualTo("application/cbor");
            handled = true;
        };
        await Pipeline(provider).ProcessAsync(args);
        await Assert.That(handled).IsTrue();
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task Binding_does_not_allocate_after_warmup(bool nativeContentType)
    {
        var args = Args(new RecordingReceiver());
        if (nativeContentType) args.Message.GetRawAmqpMessage().Properties.ContentType = "application/cbor";
        else args.Message.GetRawAmqpMessage().ApplicationProperties["content-type"] = "application/cbor";
        for (var i = 0; i < 1_000; i++) Read(args);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 10_000; i++) Read(args);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0);

        static void Read(ProcessMessageEventArgs args)
        {
            _ = ServiceBusBinding.GetBody(args);
            _ = ServiceBusBinding.GetDeliveryCount(args);
            if (!ServiceBusBinding.TryGetHeader(args, "cloudEvents:type", out _)) throw new InvalidOperationException();
            if (!ServiceBusBinding.TryGetHeader(args, "content-type", out _)) throw new InvalidOperationException();
        }
    }

    [Test]
    [Arguments("complete")]
    [Arguments("abandon")]
    [Arguments("defer")]
    [Arguments("dead_letter")]
    public async Task Manual_settlement_uses_original_native_objects_and_marks_only_after_success(string operation)
    {
        var receiver = new RecordingReceiver { Pending = new(TaskCreationOptions.RunContinuationsAsynchronously) };
        using var stop = new CancellationTokenSource();
        var args = Args(receiver, stop.Token);
        var properties = new Dictionary<string, object> { ["attempt"] = 2 };
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        NativeMessage<ProcessMessageEventArgs>? feature = null;
        control.Handle = async context =>
        {
            feature = context.Features.GetRequired<NativeMessage<ProcessMessageEventArgs>>();
            var native = context.ServiceBus();
            await Assert.That(native.Callback).IsSameReferenceAs(args);
            await Assert.That(native.Message).IsSameReferenceAs(args.Message);
            await Assert.That(context.Attempt).IsEqualTo(3);
            await Settle(native, operation, properties);
        };
        var pipeline = Pipeline(provider);
        var processing = pipeline.ProcessAsync(args, stop.Token).AsTask();
        try
        {
            await receiver.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(processing.IsCompleted).IsFalse();
            await Assert.That(feature!.Disposition.Kind).IsEqualTo(DispositionKind.Complete);
            await Assert.That(receiver.Message).IsSameReferenceAs(args.Message);
            await Assert.That(receiver.Token).IsEqualTo(stop.Token);
            await Assert.That(receiver.Operation).IsEqualTo(operation);
            if (operation != "complete") await Assert.That(receiver.Properties).IsSameReferenceAs(properties);
            if (operation == "dead_letter")
            {
                await Assert.That(receiver.Reason).IsEqualTo("poison");
                await Assert.That(receiver.Description).IsEqualTo("details");
            }
        }
        finally { receiver.Pending.SetResult(); }
        await Assert.That((await processing).Kind).IsEqualTo(DispositionKind.SettledByHandler);
        await Assert.That(feature!.Message).IsNull();
    }

    [Test]
    [Arguments("complete", false)]
    [Arguments("abandon", false)]
    [Arguments("defer", false)]
    [Arguments("dead_letter", false)]
    [Arguments("complete", true)]
    [Arguments("abandon", true)]
    [Arguments("defer", true)]
    [Arguments("dead_letter", true)]
    public async Task Failed_or_cancelled_settlement_does_not_mark_settled(string operation, bool cancel)
    {
        using var stop = new CancellationTokenSource();
        if (cancel) stop.Cancel();
        var failure = new ServiceBusException("lock lost", ServiceBusFailureReason.MessageLockLost);
        var receiver = new RecordingReceiver { Error = failure };
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        var observed = DispositionKind.SettledByHandler;
        control.Handle = async context =>
        {
            try
            {
                // Exercise cancellation inside the native call, after the pipeline starts.
                if (cancel) receiver.Error = new OperationCanceledException(stop.Token);
                await Settle(context.ServiceBus(), operation, null);
            }
            finally { observed = context.Features.GetRequired<NativeMessage<ProcessMessageEventArgs>>().Disposition.Kind; }
        };
        var processing = Pipeline(provider).ProcessAsync(Args(receiver)).AsTask();
        if (cancel) await Assert.That(async () => await processing).Throws<OperationCanceledException>();
        else await Assert.That(async () => await processing).Throws<ServiceBusException>();
        await Assert.That(observed).IsEqualTo(DispositionKind.Complete);
    }

    [Test]
    public async Task Successful_manual_settlement_rejects_a_second_settlement()
    {
        var receiver = new RecordingReceiver();
        await using var provider = Provider();
        provider.GetRequiredService<Control>().Handle = async context =>
        {
            await context.ServiceBus().DeferAsync();
            await Assert.That(async () => await context.ServiceBus().CompleteAsync()).Throws<InvalidOperationException>();
        };
        await Assert.That((await Pipeline(provider).ProcessAsync(Args(receiver))).Kind).IsEqualTo(DispositionKind.SettledByHandler);
        await Assert.That(receiver.Calls).IsEqualTo(1);
    }

    private static ValueTask Settle(ServiceBusMessageContext native, string operation, IDictionary<string, object>? properties) => operation switch
    {
        "complete" => native.CompleteAsync(),
        "abandon" => native.AbandonAsync(properties),
        "defer" => native.DeferAsync(properties),
        "dead_letter" => native.DeadLetterAsync("poison", "details", properties),
        _ => throw new ArgumentOutOfRangeException(nameof(operation)),
    };

    private static ProcessMessageEventArgs Args(ServiceBusReceiver receiver, CancellationToken token = default) => new(
        ServiceBusModelFactory.ServiceBusReceivedMessage(body: new BinaryData(new byte[] { 42 }), deliveryCount: 3,
            properties: new Dictionary<string, object> { ["cloudEvents:id"] = "id", ["cloudEvents:type"] = "servicebus.payload;v=1" }),
        receiver, "test", token);

    private static ServiceProvider Provider()
    {
        var services = new ServiceCollection();
        services.AddSingleton<Control>();
        services.AddRelayer(builder => { builder.MessageTypes.Add<Payload>("servicebus.payload"); new ConsumerBuilder<object>(builder.Services, builder.Handlers, "manual").Handler<Handler>(); });
        return services.BuildServiceProvider();
    }

    private static ReceivePipeline<ProcessMessageEventArgs, ServiceBusBinding> Pipeline(ServiceProvider provider)
    {
        var consumer = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<MessageTypeRegistry>(), new Serializer(), static (_, error, _) => ValueTask.FromException(error))
            .Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<Payload, Handler>()).Build(1);
        return new(consumer, new HeaderNames("cloudEvents:"), 1);
    }

    [Message("servicebus.payload")]
    public sealed class Payload;
    public sealed class Control { public Func<MessageContext, ValueTask> Handle { get; set; } = static _ => default; }
    public sealed class Handler(Control control) : IHandler<Payload>
    {
        public ValueTask HandleAsync(Payload message, MessageContext context, CancellationToken cancellationToken) => control.Handle(context);
    }
    private sealed class Serializer : IMessageSerializer
    {
        private readonly Payload _payload = new();
        public string ContentType => "test/cached";
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source) => (TMessage)(object)_payload;
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
    }
    private sealed class RecordingReceiver : ServiceBusReceiver
    {
        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource? Pending { get; init; }
        internal Exception? Error { get; set; }
        internal ServiceBusReceivedMessage? Message { get; private set; }
        internal CancellationToken Token { get; private set; }
        internal string? Operation { get; private set; }
        internal IDictionary<string, object>? Properties { get; private set; }
        internal string? Reason { get; private set; }
        internal string? Description { get; private set; }
        internal int Calls { get; private set; }
        private Task Record(string operation, ServiceBusReceivedMessage message, IDictionary<string, object>? properties, CancellationToken token,
            string? reason = null, string? description = null)
        {
            Calls++;
            Operation = operation;
            Message = message;
            Properties = properties;
            Token = token;
            Reason = reason;
            Description = description;
            Entered.TrySetResult();
            return Error is { } error ? Task.FromException(error) : Pending?.Task ?? Task.CompletedTask;
        }
        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
            => Record("complete", message, null, cancellationToken);
        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null,
            CancellationToken cancellationToken = default) => Record("abandon", message, propertiesToModify, cancellationToken);
        public override Task DeferMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null,
            CancellationToken cancellationToken = default) => Record("defer", message, propertiesToModify, cancellationToken);
        public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message, string deadLetterReason,
            string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default)
            => Record("dead_letter", message, null, cancellationToken, deadLetterReason, deadLetterErrorDescription);
        public override Task DeadLetterMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify,
            string deadLetterReason, string? deadLetterErrorDescription = null, CancellationToken cancellationToken = default)
            => Record("dead_letter", message, propertiesToModify, cancellationToken, deadLetterReason, deadLetterErrorDescription);
    }
}
