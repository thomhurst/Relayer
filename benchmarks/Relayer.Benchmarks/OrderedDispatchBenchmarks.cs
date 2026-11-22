using BenchmarkDotNet.Attributes;
using Relayer.Outbox.MySql;
using Relayer.Outbox.PostgreSql;
using Relayer.Outbox.SqlServer;
using Relayer.Transports;

namespace Relayer.Benchmarks;

/// <summary>Managed ordered-dispatch stages only; database, lease cadence and native I/O are excluded.</summary>
[MemoryDiagnoser]
public class OrderedDispatchBenchmarks
{
    private readonly Dictionary<string, ITransport> _transports = new(StringComparer.Ordinal);
    private readonly OrderedDispatchCapabilityCache _capabilities = new();
    private readonly NoOpTransport _transport = new();
    private OutboxRecord _row;

    [GlobalSetup]
    public void Setup()
    {
        _transports.Add(_transport.Name, _transport);
        _row = new(Guid.CreateVersion7(), _transport.Name, "benchmark.order;v=1",
            new("orders", new byte[256], new KeyValuePair<string, WireString>[] { new("ce_id", new("original-identity")) }, new WireString("customer-42"), DateTimeOffset.UtcNow),
            DateTimeOffset.UtcNow, 3);
        _ = PostgreSqlOrderedDispatcher.Prepare(_row, _transports, _capabilities);
    }

    [Benchmark]
    public OutgoingMessage ResolveAndProject()
    {
        return PostgreSqlOrderedDispatcher.Prepare(_row, _transports, _capabilities).Message;
    }

    [Benchmark]
    public OutgoingMessage MySqlResolveAndProject()
    {
        return MySqlOrderedDispatcher.Prepare(_row, _transports, _capabilities).Message;
    }

    [Benchmark]
    public OutgoingMessage SqlServerResolveAndProject()
    {
        return SqlServerOrderedDispatcher.Prepare(_row, _transports, _capabilities).Message;
    }

    [Benchmark]
    public OutboxClaim CreateClaim()
    {
        return new(_row.Id, Guid.NewGuid());
    }

    private sealed class NoOpTransport : ITransport
    {
        public string Name => "benchmark";

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            return new()
            {
                OrderedByKey = true
            };
        }

        public ValueTask SendAsync(OutgoingMessage message, CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken cancellationToken)
        {
            return new(new TopologyReport([]));
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
