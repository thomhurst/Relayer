using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Relayer.Tests;

public class SendTracingContinuationTests
{
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Native_execution_context_changes_do_not_escape_to_caller_or_completion_thread(bool batch, bool suspend)
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var local = new AsyncLocal<string?> { Value = "caller" };
        using var ambient = new Activity("caller").Start();
        var completion = new TaskCompletionSource();
        var error = new InvalidOperationException("native failure");
        var logger = new ProbeLogger(local);
        ValueTask Native()
        {
            local.Value = "native";
            return suspend ? new(completion.Task) : ValueTask.FromException(error);
        }
        var send = new SendTracing(tracing, new() { System = "test.native", Destination = "orders" },
            (_, _) => Native(), (_, _) => Native(), logger);
        OutgoingMessage[] messages = [new("orders", new byte[] { 1 })];
        var pending = batch ? send.SendBatchAsync(messages) : send.SendAsync(messages[0]);
        await Assert.That(local.Value).IsEqualTo("caller");
        await Assert.That(Activity.Current).IsSameReferenceAs(ambient);
        if (suspend)
        {
            await Task.Run(() =>
            {
                local.Value = "completion";
                completion.SetException(error);
                if (local.Value != "completion") throw new InvalidOperationException("Completion context leaked.");
            });
        }
        Exception? observed = null;
        try { await pending; }
        catch (Exception exception) { observed = exception; }
        await Assert.That(observed).IsSameReferenceAs(error);
        await Assert.That(logger.Count).IsEqualTo(1);
        await Assert.That(logger.Local).IsEqualTo("native");
        await Assert.That(logger.Activity).IsSameReferenceAs(ambient);
        await Assert.That(local.Value).IsEqualTo("caller");
        await Assert.That(Activity.Current).IsSameReferenceAs(ambient);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Completed_operation_stays_reserved_until_consumed(bool batch)
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var completion = new TaskCompletionSource();
        var send = new SendTracing(tracing, new() { System = "test.native", Destination = "orders" },
            (_, _) => new(completion.Task), (_, _) => new(completion.Task));
        OutgoingMessage[] messages = [new("orders", new byte[] { 1 })];
        var first = batch ? send.SendBatchAsync(messages) : send.SendAsync(messages[0]);
        completion.SetResult();
        await Assert.That(first.IsCompletedSuccessfully).IsTrue();
        completion = new();
        var second = batch ? send.SendBatchAsync(messages) : send.SendAsync(messages[0]);
        await first;
        await Assert.That(second.IsCompleted).IsFalse();
        completion.SetResult();
        await second;
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task Cancellation_keeps_original_exception_token_and_logs_once(bool batch, bool suspend)
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var completion = new TaskCompletionSource();
        var logger = new ProbeLogger(new());
        using var cancellation = new CancellationTokenSource();
        var error = new OperationCanceledException(cancellation.Token);
        var send = new SendTracing(tracing, new() { System = "test.native", Destination = "orders" },
            (_, _) => suspend ? new(completion.Task) : ValueTask.FromException(error),
            (_, _) => suspend ? new(completion.Task) : ValueTask.FromException(error), logger);
        OutgoingMessage[] messages = [new("orders", new byte[] { 1 })];
        var pending = batch ? send.SendBatchAsync(messages, cancellation.Token) : send.SendAsync(messages[0], cancellation.Token);
        cancellation.Cancel();
        if (suspend) completion.SetException(error);
        await Assert.That(pending.IsCanceled).IsTrue();
        Exception? observed = null;
        try { await pending; }
        catch (Exception exception) { observed = exception; }
        await Assert.That(observed).IsSameReferenceAs(error);
        await Assert.That(((OperationCanceledException)observed!).CancellationToken).IsEqualTo(cancellation.Token);
        await Assert.That(logger.Count).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Idle_slot_releases_consumed_caller_continuation(bool batch)
    {
        using var tracing = new MessageTracing(Options.Create(new TracingOptions()), []);
        var completion = new TaskCompletionSource();
        var send = new SendTracing(tracing, new() { System = "test.native", Destination = "orders" },
            (_, _) => new(completion.Task), (_, _) => new(completion.Task));
        var caller = RegisterCaller(send, completion, batch);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        await Assert.That(caller.IsAlive).IsFalse();
        GC.KeepAlive(send);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference RegisterCaller(SendTracing send, TaskCompletionSource completion, bool batch)
    {
        OutgoingMessage[] messages = [new("orders", new byte[] { 1 })];
        var pending = batch ? send.SendBatchAsync(messages) : send.SendAsync(messages[0]);
        var caller = new ConsumingCaller(pending);
        pending.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(caller.Complete);
        completion.SetResult();
        if (!caller.Consumed) throw new InvalidOperationException("The caller continuation must run inline.");
        return new(caller);
    }

    private sealed class ConsumingCaller(ValueTask pending)
    {
        internal bool Consumed;
        internal void Complete()
        {
            pending.GetAwaiter().GetResult();
            Consumed = true;
        }
    }

    private sealed class ProbeLogger(AsyncLocal<string?> local) : ILogger
    {
        internal int Count;
        internal string? Local;
        internal Activity? Activity;
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            Count++;
            Local = local.Value;
            Activity = System.Diagnostics.Activity.Current;
        }
    }
}
