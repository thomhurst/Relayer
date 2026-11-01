#pragma warning disable RLY001 // Dispatch-only fixtures use primitive messages and do not send wire messages.
using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Tests;

public class HandlerDispatchTests
{
    [Test]
    public async Task Consumer_only_registration_supports_single_batch_and_closed_generic_dispatch()
    {
        var services = new ServiceCollection();
        services.AddScoped<ScopedDependency>();
        services.AddRelayer(r => r.AddTransport<BuilderTests.BrokerOptions, BuilderTests.RouteOptions, BuilderTests.ConsumerOptions>(
            "events", b => b.Consume("orders", c => c.Handler<SingleHandler>().Handler<MultipleHandler>()
                .Handler<GenericHandler<int>>().Handler<SingleHandler>().Concurrency(2))));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        using var scope = provider.CreateScope();
        var registry = provider.GetRequiredService<HandlerRegistry>();
        var single = registry.GetHandler<int, SingleHandler>();
        var handler = (SingleHandler)single.Resolve(scope.ServiceProvider);
        var context = new TestContext(scope.ServiceProvider);
        await single.DispatchAsync(handler, 42, context, default);
        var batch = registry.GetBatchHandler<int, MultipleHandler>();
        var batchHandler = (MultipleHandler)batch.Resolve(scope.ServiceProvider);
        var messages = new Batch<int>([new(1, context), new(2, context)]);
        await registry.GetHandler<string, MultipleHandler>().DispatchAsync(batchHandler, "hello", context, default);
        var result = await batch.DispatchAsync(batchHandler, messages, default);

        await Assert.That(registry.Descriptors.Count).IsEqualTo(5);
        await Assert.That(handler.Message).IsEqualTo(42);
        await Assert.That(handler.Dependency).IsSameReferenceAs(scope.ServiceProvider.GetRequiredService<ScopedDependency>());
        await Assert.That(batchHandler.Text).IsEqualTo("hello");
        await Assert.That(result).IsSameReferenceAs(batchHandler.Result);
        var generic = registry.GetHandler<int, GenericHandler<int>>();
        await generic.DispatchAsync(generic.Resolve(scope.ServiceProvider), 7, context, default);
        await Assert.That(provider.GetRequiredService<RelayerConfiguration>().Transports[0].Consumers[0].Handlers.Count)
            .IsEqualTo(3);
    }

    [Test]
    public async Task Explicit_registration_selects_only_requested_handlers_and_is_idempotent()
    {
        var services = new ServiceCollection();
        services.AddRelayer(r => r.Handler<SingleHandler>().Handler<SingleHandler>());
        await using var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<HandlerRegistry>();

        await Assert.That(registry.Descriptors.Count).IsEqualTo(1);
        await Assert.That(registry.GetHandler<int, SingleHandler>().HandlerType).IsEqualTo(typeof(SingleHandler));
        await Assert.That(() => registry.GetHandler<int, OtherHandler>()).Throws<InvalidOperationException>();
        await Assert.That(() => registry.Add(registry.Descriptors[0])).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Discovery_registers_single_batch_inherited_and_multiple_contracts()
    {
        var services = new ServiceCollection();
        services.AddRelayer(r => r.AddHandlersFromThisAssembly());
        await using var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<HandlerRegistry>();

        await Assert.That(registry.GetHandler<int, SingleHandler>()).IsNotNull();
        await Assert.That(registry.GetHandler<int, OtherHandler>()).IsNotNull();
        await Assert.That(registry.GetHandler<int, DerivedHandler>()).IsNotNull();
        await Assert.That(registry.GetHandler<int, MultipleHandler>()).IsNotNull();
        await Assert.That(registry.GetHandler<string, MultipleHandler>()).IsNotNull();
        await Assert.That(registry.GetBatchHandler<int, MultipleHandler>().IsBatch).IsTrue();
        await Assert.That(registry.Descriptors.Any(d => d.HandlerType == typeof(AbstractHandler))).IsFalse();
    }

    [Test]
    public async Task Resolution_uses_message_scope_and_preserves_custom_registration()
    {
        var services = new ServiceCollection();
        services.AddScoped<ScopedDependency>();
        services.AddScoped<SingleHandler>(p => new SingleHandler(p.GetRequiredService<ScopedDependency>()));
        services.AddRelayer(r => r.Handler<SingleHandler>());
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var descriptor = provider.GetRequiredService<HandlerRegistry>().GetHandler<int, SingleHandler>();
        using var firstScope = provider.CreateScope();
        using var secondScope = provider.CreateScope();
        var first = (SingleHandler)descriptor.Resolve(firstScope.ServiceProvider);
        var second = (SingleHandler)descriptor.Resolve(secondScope.ServiceProvider);

        await Assert.That(descriptor.Resolve(firstScope.ServiceProvider)).IsSameReferenceAs(first);
        await Assert.That(first.Dependency).IsSameReferenceAs(firstScope.ServiceProvider.GetRequiredService<ScopedDependency>());
        await Assert.That(first).IsNotSameReferenceAs(second);
    }

    [Test]
    public async Task Typed_dispatch_forwards_message_context_cancellation_and_incomplete_ValueTask()
    {
        var services = new ServiceCollection();
        services.AddRelayer(r => r.Handler<SingleHandler>());
        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        using var cancellation = new CancellationTokenSource();
        var descriptor = provider.GetRequiredService<HandlerRegistry>().GetHandler<int, SingleHandler>();
        var handler = (SingleHandler)descriptor.Resolve(scope.ServiceProvider);
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        handler.Pending = new ValueTask(completion.Task);
        var context = new TestContext(scope.ServiceProvider);

        var pending = descriptor.DispatchAsync(handler, 42, context, cancellation.Token);

        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(handler.Message).IsEqualTo(42);
        await Assert.That(handler.Context).IsSameReferenceAs(context);
        await Assert.That(handler.Cancellation).IsEqualTo(cancellation.Token);
        completion.SetResult();
        await pending;
    }

    [Test]
    public async Task Batch_and_explicit_interface_dispatch_preserve_results_and_exceptions()
    {
        var services = new ServiceCollection();
        services.AddRelayer(r => r.Handler<MultipleHandler>());
        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var registry = provider.GetRequiredService<HandlerRegistry>();
        var single = registry.GetHandler<string, MultipleHandler>();
        var batch = registry.GetBatchHandler<int, MultipleHandler>();
        var handler = (MultipleHandler)batch.Resolve(scope.ServiceProvider);
        var context = new TestContext(scope.ServiceProvider);
        var messages = new Batch<int>([new(1, context), new(2, context)]);
        using var cancellation = new CancellationTokenSource();

        await single.DispatchAsync(handler, "hello", context, cancellation.Token);
        var result = await batch.DispatchAsync(handler, messages, cancellation.Token);

        await Assert.That(handler.Text).IsEqualTo("hello");
        await Assert.That(handler.Batch).IsSameReferenceAs(messages);
        await Assert.That(handler.Cancellation).IsEqualTo(cancellation.Token);
        await Assert.That(result).IsSameReferenceAs(handler.Result);
        await Assert.That(result.Failures[1]).IsSameReferenceAs(handler.Error);
        await Assert.That(async () => await registry.GetHandler<int, MultipleHandler>()
            .DispatchAsync(handler, 1, context, default)).Throws<InvalidOperationException>().WithMessage("handler failed");
    }

    [Test]
    public async Task Explicit_closed_generic_handlers_are_discovered()
    {
        var services = new ServiceCollection();
        services.AddRelayer(r => r.Handler<GenericHandler<int>>());
        await using var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<HandlerRegistry>();
        await Assert.That(registry.GetHandler<int, GenericHandler<int>>()).IsNotNull();
    }

    [Test]
    public async Task Unsupported_handler_registration_fails_during_configuration()
    {
        await Assert.That(() => new ServiceCollection().AddRelayer(r => r.Handler<MarkerOnly>()))
            .Throws<InvalidOperationException>().WithMessageContaining("No generated descriptor");
        await Assert.That(() => new ServiceCollection().AddRelayer(r =>
            r.AddTransport<BuilderTests.BrokerOptions, BuilderTests.RouteOptions, BuilderTests.ConsumerOptions>("events", b =>
                b.Consume("orders", c => c.Handler<MarkerOnly>()))))
            .Throws<InvalidOperationException>().WithMessageContaining("No generated descriptor");
    }

    public sealed class ScopedDependency;

    public sealed class SingleHandler(ScopedDependency? dependency = null) : IHandler<int>
    {
        public ScopedDependency? Dependency { get; } = dependency;
        public int Message { get; private set; }
        public MessageContext? Context { get; private set; }
        public CancellationToken Cancellation { get; private set; }
        public ValueTask Pending { get; set; }

        public ValueTask HandleAsync(int message, MessageContext context, CancellationToken cancellationToken)
        {
            Message = message;
            Context = context;
            Cancellation = cancellationToken;
            return Pending;
        }
    }

    public abstract class AbstractHandler : IHandler<int>
    {
        public ValueTask HandleAsync(int message, MessageContext context, CancellationToken cancellationToken) => default;
    }

    public sealed class DerivedHandler : AbstractHandler;
    public sealed class OtherHandler : AbstractHandler;
    public sealed class GenericHandler<T> : IHandler<T>
    {
        public ValueTask HandleAsync(T message, MessageContext context, CancellationToken cancellationToken) => default;
    }

    public sealed class MarkerOnly : IHandlerMarker;

    public sealed class MultipleHandler : IHandler<int>, IHandler<string>, IBatchHandler<int>
    {
        public string? Text { get; private set; }
        public Batch<int>? Batch { get; private set; }
        public CancellationToken Cancellation { get; private set; }
        public Exception Error { get; } = new InvalidOperationException("handler failed");
        public BatchResult Result { get; }

        public MultipleHandler() => Result = BatchResult.PartialFailure(new Dictionary<int, Exception> { [1] = Error });

        ValueTask IHandler<int>.HandleAsync(int message, MessageContext context, CancellationToken cancellationToken)
            => ValueTask.FromException(Error);

        ValueTask IHandler<string>.HandleAsync(string message, MessageContext context, CancellationToken cancellationToken)
        {
            Text = message;
            return default;
        }

        ValueTask<BatchResult> IBatchHandler<int>.HandleAsync(Batch<int> batch, CancellationToken cancellationToken)
        {
            Batch = batch;
            Cancellation = cancellationToken;
            return new(Result);
        }
    }

    private sealed class TestContext(IServiceProvider services) : MessageContext
    {
        public override string MessageId => "test";
        public override MessageType MessageType => new("test", 1);
        public override IReadOnlyDictionary<string, string> Headers { get; } = new Dictionary<string, string>();
        public override int Attempt => 1;
        public override IServiceProvider Services { get; } = services;
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }
}
