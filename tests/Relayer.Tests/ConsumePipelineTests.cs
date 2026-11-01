#pragma warning disable RLY001 // Low-level pipeline fixtures use primitive messages with explicit serializer metadata.
using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Serialization;

namespace Relayer.Tests;

public class ConsumePipelineTests
{
    [Test]
    public async Task Middleware_wraps_generated_dispatch_in_registration_order_and_scopes_are_fresh()
    {
        var events = new List<string>();
        var services = new ServiceCollection();
        services.AddScoped<Dependency>();
        services.AddSingleton<Control>();
        services.AddSingleton(events);
        services.AddRelayer(r => { r.MessageTypes.Add<int>("number"); r.Handler<Handler>().Handler<TextHandler>(); });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var dependencies = new List<Dependency>();
        var pipeline = Builder(provider).Handler(Descriptor(provider))
            .Use(new Middleware("outer", events, dependencies))
            .Use(new Middleware("inner", events, dependencies)).Build();

        for (var i = 0; i < 2; i++)
        {
            using var context = pipeline.RentContext(Message());
            await pipeline.ProcessAsync(context);
        }

        await Assert.That(string.Join(",", events)).IsEqualTo(
            "outer:before,inner:before,handler,inner:after,outer:after,outer:before,inner:before,handler,inner:after,outer:after");
        await Assert.That(dependencies[0]).IsSameReferenceAs(dependencies[1]);
        await Assert.That(dependencies[0]).IsNotSameReferenceAs(dependencies[2]);
        await Assert.That(dependencies.All(d => d.Disposed)).IsTrue();
    }

    [Test]
    public async Task Pending_handler_and_async_disposal_keep_scope_alive_until_both_finish()
    {
        await using var provider = Provider();
        var control = provider.GetRequiredService<Control>();
        var handlerDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var disposalDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var disposalStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        control.Invoke = _ => new ValueTask(handlerDone.Task);
        control.Disposal = new ValueTask(disposalDone.Task);
        control.DisposalStarted = disposalStarted;
        var pipeline = Builder(provider).Handler(Descriptor(provider)).Build();
        using var context = pipeline.RentContext(Message());
        var pending = pipeline.ProcessAsync(context);

        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(control.Dependency!.Disposed).IsFalse();
        await Assert.That(() => context.Dispose()).Throws<InvalidOperationException>();
        await Assert.That(() => { _ = pipeline.ProcessAsync(context); }).Throws<InvalidOperationException>();
        handlerDone.SetResult();
        await disposalStarted.Task;
        await Assert.That(pending.IsCompleted).IsFalse();
        disposalDone.SetResult();
        await pending;
        await Assert.That(control.Dependency.Disposed).IsTrue();
        await Assert.That(() => context.Services).Throws<InvalidOperationException>();
        await Assert.That(() => { _ = pipeline.ProcessAsync(context); }).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Handler_failures_use_classifier_and_failure_callback_errors_propagate()
    {
        await using var provider = Provider();
        var error = new InvalidOperationException("handler failure");
        provider.GetRequiredService<Control>().Invoke = _ => ValueTask.FromException(error);
        Exception? observed = null;
        var actions = new List<FailureAction>();
        var pipeline = Builder(provider, (_, failure, action) =>
        {
            observed = failure;
            actions.Add(action);
            return ValueTask.FromException(new ApplicationException("durable failure handling failed"));
        }).Handler(Descriptor(provider)).Build();
        using var context = pipeline.RentContext(Message());
        await Assert.That(async () => await pipeline.ProcessAsync(context)).Throws<ApplicationException>();
        await Assert.That(observed).IsSameReferenceAs(error);
        await Assert.That(actions.Single()).IsEqualTo(FailureAction.Retry);
        await Assert.That(provider.GetRequiredService<Control>().Dependency!.Disposed).IsTrue();
    }

    [Test]
    public async Task Cancellation_propagates_and_does_not_enter_failure_pipeline()
    {
        await using var provider = Provider();
        var failures = 0;
        var pipeline = Builder(provider, (_, _, _) => { failures++; return default; }).Handler(Descriptor(provider)).Build();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        using (var context = pipeline.RentContext(Message(), cancellation.Token))
            await Assert.That(async () => await pipeline.ProcessAsync(context)).Throws<OperationCanceledException>();
        await Assert.That(failures).IsEqualTo(0);

        using var handlerCancellation = new CancellationTokenSource();
        provider.GetRequiredService<Control>().Invoke = context =>
        {
            if (context.CancellationToken != handlerCancellation.Token) throw new InvalidOperationException("Wrong token.");
            handlerCancellation.Cancel();
            return ValueTask.FromCanceled(handlerCancellation.Token);
        };
        using (var context = pipeline.RentContext(Message(), handlerCancellation.Token))
            await Assert.That(async () => await pipeline.ProcessAsync(context)).Throws<OperationCanceledException>();
        await Assert.That(failures).IsEqualTo(0);
        await Assert.That(provider.GetRequiredService<Control>().Dependency!.Disposed).IsTrue();
    }

    [Test]
    public async Task Pool_reuse_clears_metadata_services_and_native_features_without_reading_headers()
    {
        await using var provider = Provider();
        var pipeline = Builder(provider).Handler(Descriptor(provider)).Build(contextPoolCapacity: 1);
        var headers = new UnreadHeaders();
        var first = pipeline.RentContext(Message() with { MessageId = "first", Attempt = 5, Headers = headers });
        first.Features.Set(new object());
        provider.GetRequiredService<Control>().Invoke = context =>
        {
            if (context.MessageId != "first" || context.Attempt != 5 || !ReferenceEquals(context.Headers, headers))
                throw new InvalidOperationException("Metadata was not forwarded.");
            return default;
        };
        await pipeline.ProcessAsync(first);
        first.Dispose();
        provider.GetRequiredService<Control>().Invoke = null;
        using var second = pipeline.RentContext(Message());
        await Assert.That(second).IsSameReferenceAs(first);
        await Assert.That(second.Features.Get<object>()).IsNull();
        await Assert.That(second.Headers.Count).IsEqualTo(0);
        await Assert.That(second.Attempt).IsEqualTo(1);
        await pipeline.ProcessAsync(second);
    }

    [Test]
    public async Task Concurrent_deliveries_have_distinct_contexts_scopes_and_handlers()
    {
        await using var provider = Provider();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dependencies = new List<Dependency>();
        var control = provider.GetRequiredService<Control>();
        control.Invoke = _ => { dependencies.Add(control.Dependency!); return new ValueTask(release.Task); };
        var pipeline = Builder(provider).Handler(Descriptor(provider)).Build(contextPoolCapacity: 1);
        using var first = pipeline.RentContext(Message());
        using var second = pipeline.RentContext(Message());
        var firstWork = pipeline.ProcessAsync(first);
        var secondWork = pipeline.ProcessAsync(second);
        await Assert.That(first).IsNotSameReferenceAs(second);
        await Assert.That(dependencies[0]).IsNotSameReferenceAs(dependencies[1]);
        release.SetResult();
        await firstWork;
        await secondWork;
        await Assert.That(dependencies.All(d => d.Disposed)).IsTrue();
    }

    [Test]
    public async Task Composition_rejects_missing_contracts_duplicate_handlers_and_late_mutation()
    {
        await using var provider = Provider();
        await Assert.That(() => Builder(provider).Build()).Throws<InvalidOperationException>();
        await Assert.That(() => Builder(provider).Handler(provider.GetRequiredService<HandlerRegistry>().GetHandler<string, TextHandler>())).Throws<InvalidOperationException>();
        var builder = Builder(provider).Handler(Descriptor(provider));
        await Assert.That(() => builder.Handler(Descriptor(provider))).Throws<InvalidOperationException>();
        var pipeline = builder.Build();
        await Assert.That(() => builder.Build()).Throws<InvalidOperationException>();
        await Assert.That(() => builder.Handler(Descriptor(provider))).Throws<InvalidOperationException>();
        var other = Builder(provider).Handler(Descriptor(provider)).Build();
        using var context = pipeline.RentContext(Message());
        await Assert.That(() => { _ = other.ProcessAsync(context); }).Throws<InvalidOperationException>();
        // Wrong-owner rejection must leave the rental usable by its owner.
        await pipeline.ProcessAsync(context);
    }

    [Test]
    public async Task Unknown_types_and_invalid_bodies_enter_failure_pipeline_as_permanent_failures()
    {
        await using var provider = Provider();
        var failures = new List<(Exception Error, FailureAction Action)>();
        var pipeline = Builder(provider, (context, error, action) =>
        {
            failures.Add((error, action));
            return default;
        }).Handler(Descriptor(provider)).Build();
        using (var unknown = pipeline.RentContext(Message() with { MessageType = new("unknown", 2) }))
            await pipeline.ProcessAsync(unknown);
        using (var invalid = pipeline.RentContext(Message() with { Body = new ReadOnlySequence<byte>(new byte[] { 0 }) }))
            await pipeline.ProcessAsync(invalid);

        await Assert.That(failures.Count).IsEqualTo(2);
        await Assert.That(failures[0].Error is UnknownMessageTypeException).IsTrue();
        await Assert.That(failures[1].Error is MessageDeserializationException).IsTrue();
        await Assert.That(failures.All(f => f.Action == FailureAction.DeadLetter)).IsTrue();
    }

    [Test]
    public async Task One_consumer_dispatches_multiple_stable_identities_and_rejects_an_unhandled_version()
    {
        var services = new ServiceCollection();
        services.AddScoped<Dependency>();
        services.AddSingleton<Control>();
        var events = new List<string>();
        services.AddSingleton(events);
        services.AddRelayer(r =>
        {
            r.MessageTypes.Add<int>("number").Add<string>("text", 2);
            r.Handler<Handler>().Handler<TextHandler>();
        });
        await using var provider = services.BuildServiceProvider();
        var failures = new List<Exception>();
        var builder = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                provider.GetRequiredService<MessageTypeRegistry>(),
                new Serializer(), (_, error, _) => { failures.Add(error); return default; });
        // Transport configuration knows only erased descriptors. Composition must not need reflection.
        foreach (var descriptor in provider.GetRequiredService<HandlerRegistry>().Descriptors) builder.Handler(descriptor);
        var pipeline = builder.Build();
        using (var number = pipeline.RentContext(Message())) await pipeline.ProcessAsync(number);
        using (var text = pipeline.RentContext(Message() with { MessageType = new("text", 2) })) await pipeline.ProcessAsync(text);
        using (var version = pipeline.RentContext(Message() with { MessageType = new("number", 2) })) await pipeline.ProcessAsync(version);
        await Assert.That(string.Join(",", events)).IsEqualTo("handler,text:hello");
        await Assert.That(failures.Count).IsEqualTo(1);
        await Assert.That(failures[0] is UnknownMessageTypeException).IsTrue();
    }

    [Test]
    public async Task Resolution_errors_are_failures_and_custom_classification_is_honored()
    {
        var services = new ServiceCollection();
        services.AddScoped<Handler>(_ => throw new InvalidOperationException("resolution failed"));
        services.AddRelayer(r => { r.MessageTypes.Add<int>("number"); r.Handler<Handler>().Handler<TextHandler>(); });
        await using var provider = services.BuildServiceProvider();
        Exception? failure = null;
        FailureAction? result = null;
        var pipeline = new ConsumePipelineBuilder(provider.GetRequiredService<IServiceScopeFactory>(),
                provider.GetRequiredService<MessageTypeRegistry>(),
                new Serializer(), (_, error, action) => { failure = error; result = action; return default; }, new Classifier())
            .Handler(Descriptor(provider)).Build();
        using var context = pipeline.RentContext(Message());
        await pipeline.ProcessAsync(context);
        await Assert.That(failure!.Message).IsEqualTo("resolution failed");
        await Assert.That(result).IsEqualTo(FailureAction.Discard);
    }

    [Test]
    public async Task Scope_disposal_failure_propagates_and_does_not_report_successful_failure_handling()
    {
        await using var provider = Provider();
        provider.GetRequiredService<Control>().Disposal = ValueTask.FromException(new ApplicationException("disposal failed"));
        var failures = 0;
        var pipeline = Builder(provider, (_, _, _) => { failures++; return default; }).Handler(Descriptor(provider)).Build();
        using var context = pipeline.RentContext(Message());
        await Assert.That(async () => await pipeline.ProcessAsync(context)).Throws<ApplicationException>().WithMessage("disposal failed");
        await Assert.That(failures).IsEqualTo(0);
        await Assert.That(() => context.Services).Throws<InvalidOperationException>();
    }

    internal static ConsumeMessage Message() => new()
    {
        MessageId = "id", MessageType = new("number", 1),
        Body = new ReadOnlySequence<byte>(new byte[] { 42 }),
    };

    private static ServiceProvider Provider()
    {
        var services = new ServiceCollection();
        services.AddScoped<Dependency>();
        services.AddSingleton<Control>();
        services.AddSingleton(new List<string>());
        services.AddRelayer(r => { r.MessageTypes.Add<int>("number"); r.Handler<Handler>().Handler<TextHandler>(); });
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static HandlerDescriptor Descriptor(ServiceProvider provider)
        => provider.GetRequiredService<HandlerRegistry>().GetHandler<int, Handler>();

    private static ConsumePipelineBuilder Builder(ServiceProvider provider, ConsumeFailureDelegate? failure = null)
        => new(provider.GetRequiredService<IServiceScopeFactory>(), provider.GetRequiredService<MessageTypeRegistry>(),
            new Serializer(),
            failure ?? (static (_, error, _) => ValueTask.FromException(error)));

    public sealed class Dependency : IAsyncDisposable
    {
        public bool Disposed { get; private set; }
        public ValueTask PendingDisposal { get; set; }
        public TaskCompletionSource? DisposalStarted { get; set; }
        public async ValueTask DisposeAsync()
        {
            DisposalStarted?.SetResult();
            await PendingDisposal;
            Disposed = true;
        }
    }

    public sealed class Control
    {
        public Func<MessageContext, ValueTask>? Invoke { get; set; }
        public Dependency? Dependency { get; set; }
        public ValueTask Disposal { get; set; }
        public TaskCompletionSource? DisposalStarted { get; set; }
    }

    public sealed class Handler(Dependency dependency, List<string> events, Control control) : IHandler<int>
    {
        public ValueTask HandleAsync(int message, MessageContext context, CancellationToken cancellationToken)
        {
            if (message != 42 || dependency.Disposed || !ReferenceEquals(context.Services.GetRequiredService<Dependency>(), dependency))
                throw new InvalidOperationException("Invalid message scope.");
            events.Add("handler");
            control.Dependency = dependency;
            dependency.PendingDisposal = control.Disposal;
            dependency.DisposalStarted = control.DisposalStarted;
            return control.Invoke?.Invoke(context) ?? default;
        }
    }

    public sealed class TextHandler(List<string> events) : IHandler<string>
    {
        public ValueTask HandleAsync(string message, MessageContext context, CancellationToken cancellationToken)
        {
            events.Add("text:" + message);
            return default;
        }
    }

    private sealed class Classifier : IFailureClassifier
    {
        public FailureAction Classify(Exception exception, MessageContext context) => FailureAction.Discard;
    }

    private sealed class UnreadHeaders : IReadOnlyDictionary<string, string>
    {
        public int Count => throw new InvalidOperationException("Headers were read eagerly.");
        public IEnumerable<string> Keys => throw new NotSupportedException();
        public IEnumerable<string> Values => throw new NotSupportedException();
        public string this[string key] => throw new NotSupportedException();
        public bool ContainsKey(string key) => throw new NotSupportedException();
        public bool TryGetValue(string key, out string value) => throw new NotSupportedException();
        public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => throw new NotSupportedException();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class Middleware(string name, List<string> events, List<Dependency> dependencies) : IConsumeMiddleware
    {
        public async ValueTask InvokeAsync(MessageContext context, ConsumeDelegate next)
        {
            dependencies.Add(context.Services.GetRequiredService<Dependency>());
            events.Add(name + ":before");
            await next(context);
            events.Add(name + ":after");
        }
    }

    internal sealed class Serializer : IMessageSerializer
    {
        public string ContentType => "test/byte";
        public void Serialize<TMessage>(TMessage message, IBufferWriter<byte> destination) => throw new NotSupportedException();
        public TMessage Deserialize<TMessage>(ReadOnlySequence<byte> source)
        {
            if (source.FirstSpan[0] == 0) throw new FormatException("Invalid body.");
            // Test serializer only. The pipeline itself retains typed values without boxing.
            return typeof(TMessage) == typeof(string) ? (TMessage)(object)"hello" : (TMessage)(object)(int)source.FirstSpan[0];
        }
    }
}
