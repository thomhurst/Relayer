using Microsoft.Extensions.DependencyInjection;

namespace Relayer.Sagas.Tests;

public class SagaExecutionTests
{
    [Test]
    [Arguments("load", false)]
    [Arguments("load", true)]
    [Arguments("transition", false)]
    [Arguments("transition", true)]
    [Arguments("save", false)]
    [Arguments("save", true)]
    public async Task Every_stage_supports_synchronous_and_suspended_success(string stage, bool asynchronous)
    {
        await using var services = Services();
        var loaded = new TaskCompletionSource<Data?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var state = NewData();
        var loads = 0;
        var transitions = 0;
        var saves = 0;
        var handler = new Process
        {
            Load = () => { loads++; return stage == "load" && asynchronous ? new(loaded.Task) : new(state); },
            Transition = _ => { transitions++; return stage == "transition" && asynchronous ? new(completed.Task) : default; },
            Save = (_, _) => { saves++; return stage == "save" && asynchronous ? new(completed.Task) : default; }
        };
        var work = handler.HandleAsync(new(), new Context(services), default);
        await Assert.That(work.IsCompleted).IsEqualTo(!asynchronous);
        if (asynchronous)
        {
            if (stage == "load") loaded.SetResult(state);
            else completed.SetResult();
        }
        await work;
        await Assert.That(loads).IsEqualTo(1);
        await Assert.That(transitions).IsEqualTo(1);
        await Assert.That(saves).IsEqualTo(1);
    }

    [Test]
    [Arguments("validate", false)]
    [Arguments("load", false)]
    [Arguments("load", true)]
    [Arguments("transition", false)]
    [Arguments("transition", true)]
    [Arguments("save", false)]
    [Arguments("save", true)]
    public async Task Synchronous_throws_and_completed_faults_release_execution_state(string stage, bool faultedTask)
    {
        await using var services = Services();
        var error = new InvalidOperationException("Synchronous stage failure.");
        var handler = new Process();
        switch (stage)
        {
            case "validate": handler.Validate = () => throw error; break;
            case "load": handler.Load = () => faultedTask ? ValueTask.FromException<Data?>(error) : throw error; break;
            case "transition": handler.Transition = _ => faultedTask ? ValueTask.FromException(error) : throw error; break;
            case "save": handler.Save = (_, _) => faultedTask ? ValueTask.FromException(error) : throw error; break;
        }
        var context = new Context(services);
        var work = handler.HandleAsync(new(), context, default);
        await Assert.That(work.IsCompleted).IsTrue();
        await Assert.That(async () => await work).Throws<InvalidOperationException>().WithMessage(error.Message);
        await new Process().HandleAsync(new(), context, default);
    }

    [Test]
    public async Task Completed_operation_stays_reserved_until_GetResult_is_consumed()
    {
        await using var services = Services();
        // Inline completion makes the first operation's finished-but-unconsumed interval deterministic.
        var firstLoad = new TaskCompletionSource<Data?>();
        var secondLoad = new TaskCompletionSource<Data?>();
        var first = new Process { Load = () => new(firstLoad.Task) };
        var second = new Process { Load = () => new(secondLoad.Task) };
        var context = new Context(services);
        var firstWork = first.HandleAsync(new(), context, default);
        firstLoad.SetResult(NewData());
        await Assert.That(firstWork.IsCompletedSuccessfully).IsTrue();
        var secondWork = second.HandleAsync(new(), context, default);
        // Returning the first operation at completion rather than consumption would invalidate its token here.
        await firstWork;
        secondLoad.SetResult(NewData());
        await secondWork;
        await new Process().HandleAsync(new(), context, default);
    }

    [Test]
    public async Task Cancellation_of_suspended_load_releases_execution_state()
    {
        await using var services = Services();
        var loaded = new TaskCompletionSource<Data?>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        var handler = new Process { Load = () => new(loaded.Task) };
        var context = new Context(services);
        var work = handler.HandleAsync(new(), context, cancellation.Token).AsTask();
        cancellation.Cancel();
        loaded.SetCanceled(cancellation.Token);
        await Assert.That(async () => await work).Throws<TaskCanceledException>();
        await new Process().HandleAsync(new(), context, default);
    }

    [Test]
    [Arguments("load")]
    [Arguments("transition")]
    [Arguments("save")]
    public async Task Suspended_stages_propagate_errors_and_release_state_for_next_delivery(string stage)
    {
        await using var services = Services();
        var context = new Context(services);
        var loaded = new TaskCompletionSource<Data?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new Process
        {
            Load = stage == "load" ? () => { entered.SetResult(); return new(loaded.Task); } : static () => new(NewData()),
            Transition = stage == "transition" ? _ => { entered.SetResult(); return new(completed.Task); } : static _ => default,
            Save = stage == "save" ? (_, _) => { entered.SetResult(); return new(completed.Task); } : static (_, _) => default
        };
        var work = handler.HandleAsync(new(), context, default).AsTask();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var error = new InvalidOperationException("Suspended stage failed.");
        if (stage == "load") loaded.SetException(error);
        else completed.SetException(error);
        await Assert.That(async () => await work).Throws<InvalidOperationException>().WithMessage(error.Message);

        var saves = 0;
        var next = new Process { Save = (_, _) => { saves++; return default; } };
        await next.HandleAsync(new(), context, default);
        await Assert.That(saves).IsEqualTo(1);
    }

    [Test]
    public async Task Suspended_application_code_keeps_its_execution_context_without_changing_the_caller()
    {
        await using var services = Services();
        var local = new AsyncLocal<string?> { Value = "caller" };
        var loaded = new TaskCompletionSource<Data?>(TaskCreationOptions.RunContinuationsAsynchronously);
        string? observed = null;
        var handler = new Process
        {
            Validate = () => local.Value = "transition",
            Load = () => new(loaded.Task),
            Transition = _ => { observed = local.Value; return default; }
        };
        var work = handler.HandleAsync(new(), new Context(services), default);
        await Assert.That(local.Value).IsEqualTo("caller");
        loaded.SetResult(NewData());
        await work;
        await Assert.That(observed).IsEqualTo("transition");
        await Assert.That(local.Value).IsEqualTo("caller");
    }

    [Test]
    public async Task Overlapping_sagas_share_the_pool_without_sharing_delivery_state()
    {
        await using var services = Services();
        var firstLoad = new TaskCompletionSource<Data?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondLoad = new TaskCompletionSource<Data?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstState = NewData();
        var secondState = NewData();
        Data? firstSaved = null, secondSaved = null;
        var first = new Process { Load = () => new(firstLoad.Task), Save = (state, _) => { firstSaved = state; return default; } };
        var second = new Process { Load = () => new(secondLoad.Task), Save = (state, _) => { secondSaved = state; return default; } };
        var firstWork = first.HandleAsync(new(), new Context(services), default);
        var secondWork = second.HandleAsync(new(), new Context(services), default);
        secondLoad.SetResult(secondState);
        await secondWork;
        firstLoad.SetResult(firstState);
        await firstWork;
        await Assert.That(firstSaved).IsSameReferenceAs(firstState);
        await Assert.That(secondSaved).IsSameReferenceAs(secondState);
    }

    [Test]
    public async Task New_state_is_marked_new_and_completed_state_skips_application_work()
    {
        await using var services = Services();
        var isNew = false;
        var handler = new Process
        {
            Load = static () => new((Data?)null),
            Save = (_, created) => { isNew = created; return default; }
        };
        await handler.HandleAsync(new(), new Context(services), default);
        await Assert.That(isNew).IsTrue();
        handler.Load = static () => new(new Data { Id = Guid.NewGuid(), IsCompleted = true });
        handler.Transition = static _ => throw new InvalidOperationException("Completed state must not transition.");
        handler.Save = static (_, _) => throw new InvalidOperationException("Completed state must not save.");
        await handler.HandleAsync(new(), new Context(services), default);
    }

    [Test]
    public async Task Cancellation_during_an_asynchronous_save_is_preserved()
    {
        await using var services = Services();
        var saved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        var handler = new Process { Save = (_, _) => new(saved.Task) };
        var work = handler.HandleAsync(new(), new Context(services), cancellation.Token).AsTask();
        cancellation.Cancel();
        saved.SetCanceled(cancellation.Token);
        await Assert.That(async () => await work).Throws<TaskCanceledException>();
        // A subsequent delivery can reuse the execution state after cancellation.
        await new Process().HandleAsync(new(), new Context(services), default);
    }

    private static ServiceProvider Services() => new ServiceCollection().AddSingleton<SagaExecution<Data, Input>>().BuildServiceProvider();
    private static Data NewData() => new() { Id = Guid.NewGuid() };
    private sealed class Data : SagaData;
    private sealed class Input;
    private sealed class Process : Saga<Data, Input>
    {
        internal Action? Validate;
        internal Func<ValueTask<Data?>> Load = static () => new(NewData());
        internal Func<Data, ValueTask> Transition = static _ => default;
        internal Func<Data, bool, ValueTask> Save = static (_, _) => default;
        protected override void ValidateTransaction() => Validate?.Invoke();
        protected override ValueTask<Data?> LoadAsync(Input message, CancellationToken cancellationToken) => Load();
        protected override Data Create(Input message) => NewData();
        protected override ValueTask TransitionAsync(Data state, Input message, MessageContext context, CancellationToken cancellationToken) => Transition(state);
        protected override ValueTask SaveAsync(Data state, bool isNew, CancellationToken cancellationToken) => Save(state, isNew);
    }
    private sealed class Context(IServiceProvider services) : MessageContext
    {
        public override string MessageId => "018f0000-0000-7000-8000-000000000001";
        public override MessageType MessageType => new("saga.execution", 1);
        public override MessageHeaders Headers => MessageHeaders.Empty;
        public override int Attempt => 1;
        public override IServiceProvider Services => services;
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }
}
