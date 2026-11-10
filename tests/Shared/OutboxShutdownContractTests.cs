using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Relayer.Transports;

namespace Relayer.TestFixtures;

// Providers adopt each case when its corresponding worker ships. All state belongs to one host.
public abstract class OutboxShutdownContractTests
{
    protected abstract Task<ShutdownFixture> CreateFixtureAsync(string worker, ShutdownTransport transport,
        ShutdownLogs logs);

    protected abstract class ShutdownFixture : IAsyncDisposable
    {
        public abstract Task StartAsync();
        public abstract Task VerifyInFlightAsync();
        public abstract Task StopAsync(CancellationToken token);
        public abstract Task VerifyDurableStateAsync();
        public abstract ValueTask DisposeHostAsync();
        public abstract Task VerifyClosedAsync();
        public abstract ValueTask DisposeAsync();
    }

    [Test]
    [Arguments("direct")]
    [Arguments("ordered")]
    [Arguments("maintenance")]
    public async Task Graceful_host_stop_drains_before_resources_close_and_preserves_durable_work(string worker)
    {
        var logs = new ShutdownLogs();
        var transport = new ShutdownTransport(worker != "direct");
        await using var fixture = await CreateFixtureAsync(worker, transport, logs);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try
        {
            await fixture.StartAsync();
            await transport.Entered.Task.WaitAsync(deadline.Token);
            await fixture.VerifyInFlightAsync();
            var stop = fixture.StopAsync(deadline.Token);
            await transport.Stopping.Task.WaitAsync(deadline.Token);
            // Stopping has begun, but native completion has not: sender and database must still be usable.
            await Assert.That(stop.IsCompleted).IsFalse();
            await Assert.That(transport.Closed).IsFalse();
            transport.Release.TrySetResult();
            await stop.WaitAsync(deadline.Token);
            await Assert.That(transport.Active).IsEqualTo(0);
            await Assert.That(transport.Closed).IsTrue();
            await fixture.VerifyDurableStateAsync();
            await fixture.DisposeHostAsync();
            await fixture.VerifyClosedAsync();
            await Assert.That(transport.Disposed).IsTrue();
            await Assert.That(transport.SendsAfterClose).IsEqualTo(0);
            await Assert.That(logs.Failures.ToArray()).IsEmpty();
        }
        finally { transport.Release.TrySetResult(); }
    }

    public sealed class ShutdownLogs : ILoggerProvider
    {
        public ConcurrentQueue<string> Failures { get; } = new();
        public ConcurrentQueue<string> DatabaseCommands { get; } = new();
        public bool DatabaseClosed { get; set; }
        public string? DatabaseCommandCategory { get; set; }
        public ILogger CreateLogger(string categoryName) => new Capture(this, categoryName);
        public void Dispose() { }

        private sealed class Capture(ShutdownLogs owner, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;
            public void Log<TState>(LogLevel level, EventId id, TState state, Exception? error,
                Func<TState, Exception?, string> format)
            {
                var text = format(state, error);
                if (owner.DatabaseCommandCategory is { } databaseCategory && category.StartsWith(databaseCategory, StringComparison.Ordinal))
                {
                    owner.DatabaseCommands.Enqueue(text);
                    if (owner.DatabaseClosed) owner.Failures.Enqueue("Database command after disposal: " + text);
                }
                if (level >= LogLevel.Error || level >= LogLevel.Warning &&
                    (error is OperationCanceledException || text.Contains("cancel", StringComparison.OrdinalIgnoreCase)))
                    owner.Failures.Enqueue($"{category}: {level}: {text} {error}");
            }
        }
    }

    public sealed class ShutdownTransport(bool cancelSend) : ITransport
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Stopping { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool Closed { get; private set; }
        public bool Disposed { get; private set; }
        public int Active;
        public int SendsAfterClose;
        public int Delivered;
        public string Name => "events";
        public TransportCapabilities GetCapabilities(string endpoint) => new() { OrderedByKey = true, MaxBatchSize = 100 };
        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken token)
        {
            if (Closed) Interlocked.Increment(ref SendsAfterClose);
            Interlocked.Increment(ref Active);
            Entered.TrySetResult();
            try
            {
                if (cancelSend)
                {
                    try { await Task.Delay(Timeout.InfiniteTimeSpan, token); }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        Stopping.TrySetResult();
                        await Release.Task; // Native operation is still unwinding; stop must join it.
                        throw;
                    }
                }
                else await Release.Task;
                Interlocked.Increment(ref Delivered);
            }
            finally { Interlocked.Decrement(ref Active); }
        }
        public async ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token)
        {
            for (var i = 0; i < messages.Length; i++) await SendAsync(messages.Span[i], token);
        }
        public Task StartAsync(CancellationToken token) => Task.CompletedTask;
        public Task StopAsync(CancellationToken token)
        {
            if (Active != 0) throw new InvalidOperationException("Sender closed before native send completed.");
            Closed = true;
            return Task.CompletedTask;
        }
        public ValueTask DisposeAsync()
        {
            if (!Closed || Active != 0) throw new InvalidOperationException("Sender disposed before graceful stop.");
            Disposed = true;
            return default;
        }
        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken token) => new(new TopologyReport([]));
    }
}
