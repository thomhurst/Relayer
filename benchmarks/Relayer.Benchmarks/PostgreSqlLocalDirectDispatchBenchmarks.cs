using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Relayer.Outbox.PostgreSql;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Measures hosted handoff, claims and fenced completion for a pair of independently committed local rows.</summary>
[MemoryDiagnoser]
public class PostgreSqlLocalDirectDispatchBenchmarks
{
    private readonly PostgreSqlOutboxOptions _options = new() { Schema = "local_direct_" + Guid.NewGuid().ToString("N") };
    private readonly List<BufferedOutboxMessage> _rows = [new(), new()];
    private readonly List<BufferedOutboxMessage> _barrierRows = [new()];
    private NpgsqlDataSource _source = null!;
    private PostgreSqlLocalTransport _transport = null!;
    private PostgreSqlDirectDispatcher _worker = null!;
    private TaskCompletionSource _settled = null!;
    private TaskCompletionSource _barrierSettled = null!;
    private int _remaining;

    [Params(1, 2)]
    public int Concurrency
    {
        get; set;
    }

    [Params(false, true)]
    public bool Saturated
    {
        get;
        set;
    }

    [GlobalSetup]
    public void Setup()
    {
        _source = NpgsqlDataSource.Create(Environment.GetEnvironmentVariable("RELAYER_POSTGRES_BENCHMARK_CONNECTION")
            ?? throw new InvalidOperationException("Set RELAYER_POSTGRES_BENCHMARK_CONNECTION to an isolated PostgreSQL database."));
        using var create = _source.CreateCommand(PostgreSqlOutboxSchema.CreateScript(_options));
        create.ExecuteNonQuery();
        _transport = new("local", _source, _options, ["work", "work-other"]);
        _transport.Register("work", SettleAsync, Concurrency, atLeastOnce: true);
        _transport.Register("work-other", SettleAsync, atLeastOnce: true);
        _worker = new(_source, _options, new(), [_transport], NullLogger<PostgreSqlDirectDispatcher>.Instance);
        _worker.StartAsync(default).GetAwaiter().GetResult();
        Populate();
        DispatchPair().GetAwaiter().GetResult();
    }

    [IterationSetup]
    public void Populate()
    {
        // The settlement signal precedes admission release. Wait outside measurement for all slots.
        var receiver = _transport.GetReceiver("work")!;
        var admitted = 0;

        try
        {
            var spinner = new SpinWait();

            while (admitted < Concurrency)
            {
                if (receiver.TryEnter())
                {
                    admitted++;
                }
                else
                {
                    spinner.SpinOnce();
                }
            }
        }
        finally
        {
            for (var i = 0; i < admitted; i++)
            {
                receiver.Exit();
            }
        }

        foreach (var row in _rows)
        {
            PopulateRow(row, "work");
        }

        if (Saturated)
        {
            PopulateRow(_barrierRows[0], "work-other");
            _barrierSettled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        _remaining = _rows.Count;
        _settled = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private void PopulateRow(BufferedOutboxMessage row, string destination)
    {
        var id = Guid.CreateVersion7();
        var now = DateTimeOffset.UtcNow;
        using var insert = _source.CreateCommand($"""
            INSERT INTO {_options.Schema}.relayer_outbox (id, transport, destination, message_type, headers, body, created_at)
            VALUES (@id, 'local', @destination, 'benchmark;v=1', ''::bytea, ''::bytea, @now)
            """);
        insert.Parameters.AddWithValue("id", id);
        insert.Parameters.AddWithValue("destination", destination);
        insert.Parameters.AddWithValue("now", now);
        insert.ExecuteNonQuery();
        row.Copy(new(id, "local", "benchmark;v=1", new(destination, default), now), 1);
    }

    [Benchmark(OperationsPerInvoke = 2)]
    public Task DispatchPair()
    {
        if (Saturated)
        {
            return DispatchSaturatedPairAsync();
        }

        CommitPair();
        return _settled.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    private void CommitPair()
    {
        _worker.Committed(_rows, 1);
        // Take swaps the first row in place. Hand off the second independently.
        _rows.Reverse();
        _worker.Committed(_rows, 1);
    }

    private async Task DispatchSaturatedPairAsync()
    {
        var receiver = _transport.GetReceiver("work")!;
        var admitted = 0;

        try
        {
            for (; admitted < Concurrency; admitted++)
            {
                if (!receiver.TryEnter())
                {
                    throw new InvalidOperationException("The previous benchmark iteration still holds admission.");
                }
            }

            CommitPair();
            // An unrelated destination confirms the reader reached both rejected rows.
            _worker.Committed(_barrierRows, 1);
            await _barrierSettled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            for (var i = 0; i < admitted; i++)
            {
                receiver.Exit();
            }
        }

        await _settled.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<Disposition> SettleAsync(PostgreSqlLocalDelivery delivery, CancellationToken cancellationToken)
    {
        await _transport.SettleAsync(delivery, Disposition.Complete, cancellationToken);

        if (delivery.Message.Destination == "work-other")
        {
            _barrierSettled.SetResult();
        }
        else if (Interlocked.Decrement(ref _remaining) == 0)
        {
            _settled.SetResult();
        }

        return Disposition.SettledByHandler;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _worker.StopAsync(default);
        _worker.Dispose();
        await _transport.DisposeAsync();
        await using var drop = _source.CreateCommand($"DROP SCHEMA {_options.Schema} CASCADE");
        await drop.ExecuteNonQueryAsync();
        await _source.DisposeAsync();
    }
}
