using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;
using Npgsql;

namespace Relayer.Outbox.PostgreSql;

// One operation per scoped store. Native task/command allocations stay in Npgsql;
// asynchronous result conversion reuses this source rather than allocating an async state machine per row.
internal sealed class ScalarOperation<T>(Func<object?, T> convert) : IValueTaskSource<T>
{
    private ManualResetValueTaskSourceCore<T> _source;
    private ConfiguredTaskAwaitable<object?>.ConfiguredTaskAwaiter _pending;
    private NpgsqlCommand? _command;
    private Action? _continuation;

    internal ValueTask<T> Start(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        if (_command is not null)
        {
            command.Dispose();
            throw new InvalidOperationException("A PostgreSQL outbox store operation is already active. Await each operation before starting the next.");
        }
        _command = command;
        try
        {
            _pending = command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false).GetAwaiter();
            if (_pending.IsCompleted)
            {
                var result = convert(_pending.GetResult());
                Cleanup();
                return new(result);
            }
            _source.Reset();
            var version = _source.Version;
            _pending.UnsafeOnCompleted(_continuation ??= Complete);
            return new(this, version);
        }
        catch
        {
            Cleanup();
            throw;
        }
    }

    private void Complete()
    {
        T result;
        try
        {
            result = convert(_pending.GetResult());
            // Keep the operation reserved until GetResult, even with asynchronous caller continuations.
            _command!.Dispose();
            _pending = default;
        }
        catch (Exception error)
        {
            _command!.Dispose();
            _pending = default;
            _source.SetException(error);
            return;
        }
        _source.SetResult(result);
    }

    private void Cleanup()
    {
        _command?.Dispose();
        _command = null;
        _pending = default;
    }

    public T GetResult(short token)
    {
        try { return _source.GetResult(token); }
        finally { _command = null; }
    }
    public ValueTaskSourceStatus GetStatus(short token) => _source.GetStatus(token);
    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _source.OnCompleted(continuation, state, token, flags);
}
