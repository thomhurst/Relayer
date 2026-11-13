using Microsoft.Extensions.DependencyInjection;
using Relayer;
using Relayer.Sagas;

// Exercises inherited generated dispatch and the provider-independent transition runtime.
// Native persistence and concurrency are covered by PostgreSQL SagaTests.
internal static class SagaDispatchSmoke
{
    internal static async Task RunAsync()
    {
        var services = new ServiceCollection();
        services.AddSingleton<SmokeSagaStore>();
        services.AddRelayer(r =>
        {
            r.MessageTypes.Add<SmokeSagaMessage>("smoke.saga");
            r.AddTransport<object, object, object>("manual-1", transport => transport.Consume("source", consumer => consumer.Handler<SmokeSaga>()));
        });
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var descriptor = provider.GetRequiredService<HandlerRegistry>().GetHandler<SmokeSagaMessage, SmokeSaga>();
        await descriptor.DispatchAsync(descriptor.Resolve(scope.ServiceProvider), new(), new Context(scope.ServiceProvider), default);
        var store = provider.GetRequiredService<SmokeSagaStore>();
        if (store.Saves != 1 || store.State is not { Count: 1 })
            throw new InvalidOperationException("Saga generated dispatch failed under Native AOT.");
        Console.WriteLine("Saga generated dispatch passed under Native AOT.");
    }

    private sealed class Context(IServiceProvider services) : MessageContext
    {
        public override string MessageId => "018f0000-0000-7000-8000-000000000001";
        public override MessageType MessageType => new("smoke.saga", 1);
        public override MessageHeaders Headers => MessageHeaders.Empty;
        public override int Attempt => 1;
        public override IServiceProvider Services => services;
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }
}

[Message("smoke.saga")]
public sealed class SmokeSagaMessage;
public sealed class SmokeSagaState : SagaData { public int Count { get; set; } }
public sealed class SmokeSagaStore
{
    public int Saves;
    public SmokeSagaState? State;
}
public sealed class SmokeSaga(SmokeSagaStore store) : Saga<SmokeSagaState, SmokeSagaMessage>
{
    protected override void ValidateTransaction() { }
    protected override ValueTask<SmokeSagaState?> LoadAsync(SmokeSagaMessage message, CancellationToken cancellationToken) => new(store.State);
    protected override SmokeSagaState Create(SmokeSagaMessage message) => new() { Id = Guid.NewGuid() };
    protected override ValueTask TransitionAsync(SmokeSagaState state, SmokeSagaMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        state.Count++;
        return default;
    }
    protected override ValueTask SaveAsync(SmokeSagaState state, bool isNew, CancellationToken cancellationToken)
    {
        store.State = state;
        store.Saves++;
        return default;
    }
}
