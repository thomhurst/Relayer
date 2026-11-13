using Microsoft.Extensions.Logging;
using Npgsql;
using Relayer.Testing;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql.Tests;

[ClassDataSource<PostgreSqlLocalSessionFixture>(Shared = SharedType.PerTestSession)]
public class LocalTransportConformanceTests(PostgreSqlLocalSessionFixture server)
{
    [Test]
    [Arguments(TransportConformanceCase.SendReceiveSettle)]
    [Arguments(TransportConformanceCase.OrderingByKey)]
    [Arguments(TransportConformanceCase.TracePropagation)]
    [Arguments(TransportConformanceCase.CreationContextCopies)]
    [Arguments(TransportConformanceCase.Reconnect)]
    [Arguments(TransportConformanceCase.CleanShutdown)]
    [Arguments(TransportConformanceCase.Retry)]
    [Arguments(TransportConformanceCase.DeadLetter)]
    [Arguments(TransportConformanceCase.Delay)]
    [Arguments(TransportConformanceCase.RedeliveryDelay)]
    public async Task Shared_transport_contract(TransportConformanceCase scenario)
    {
        var suite = new TransportConformanceSuite<PostgreSqlLocalDelivery, PostgreSqlLocalMessageBinding>(async (tracing, _) =>
        {
            var fixture = new Fixture(tracing, server);
            try { await fixture.Initialize(); return fixture; }
            catch { await fixture.DisposeAsync(); throw; }
        });
        var result = await suite.RunAsync(scenario);
        await Assert.That(result.Skipped).IsEqualTo(scenario is TransportConformanceCase.Retry or TransportConformanceCase.DeadLetter
            or TransportConformanceCase.Delay or TransportConformanceCase.RedeliveryDelay);
    }

    private sealed class Fixture : ITransportConformanceFixture<PostgreSqlLocalDelivery>, ITransportShutdownConformanceFixture
    {
        private readonly PostgreSqlLocalSessionFixture.Database _database;
        private readonly MessageTracing _tracing;
        private NpgsqlDataSource _source = null!;
        private PostgreSqlLocalTransport _local = null!;
        private readonly ILoggerFactory _logs;
        private Lifecycle _lifecycle = null!;
        public Fixture(MessageTracing tracing, PostgreSqlLocalSessionFixture server)
        {
            _database = server.CreateDatabase();
            _tracing = tracing;
            _logs = LoggerFactory.Create(logging => logging.AddProvider(Logs));
        }
        public ConformanceLogCapture Logs { get; } = new();
        public ITransport Transport => _lifecycle;
        public string Destination => "work";
        public string MessagingSystem => "postgresql";
        public string SettlementOperation => "complete";
        public HeaderNames HeaderNames { get; } = new();
        public async Task Initialize()
        {
            await _database.StartAsync();
            _source = NpgsqlDataSource.Create(_database.ConnectionString);
            await PostgreSqlOutboxStoreTests.CreateSchema(_source);
            _local = new("local", _source, new() { Lanes = 1 }, [Destination], _tracing);
            _lifecycle = new(_local, _source, _logs);
        }
        public void RegisterConsumer(Func<PostgreSqlLocalDelivery, CancellationToken, ValueTask<Disposition>> receive) => _local.Register(Destination, receive);
        public ValueTask SettleAsync(PostgreSqlLocalDelivery incoming, Disposition disposition, CancellationToken cancellationToken) =>
            _local.SettleAsync(incoming, disposition, cancellationToken);
        public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task DisconnectAsync(CancellationToken cancellationToken) => _lifecycle.StopAsync(cancellationToken);
        public Task ReconnectAsync(CancellationToken cancellationToken) => _lifecycle.StartAsync(cancellationToken);
        public async ValueTask VerifyShutdownAsync(CancellationToken cancellationToken)
        {
            await Assert.That(await LocalTransportTests.Count(_source, "relayer_outbox")).IsEqualTo(0L);
            Logs.AssertClean();
        }
        public async ValueTask DisposeAsync()
        {
            if (_lifecycle is not null) await _lifecycle.DisposeAsync();
            if (_local is not null) await _local.DisposeAsync();
            if (_source is not null) await _source.DisposeAsync();
            _logs.Dispose();
            await _database.DisposeAsync();
        }

        // Application lifecycle composition only: every queue, claim and settlement remains production code.
        private sealed class Lifecycle(PostgreSqlLocalTransport local, NpgsqlDataSource source, ILoggerFactory logs) : ITransport, ITransportWrapper, ITransportSendTracing
        {
            private PostgreSqlDirectDispatcher? _direct;
            private PostgreSqlMaintenanceWorker? _recovery;
            private PostgreSqlOrderedDispatcher? _ordered;
            public ITransport InnerTransport => local;
            public string Name => local.Name;
            public TransportCapabilities GetCapabilities(string endpoint) => local.GetCapabilities(endpoint);
            public SendTracing? GetSendTracing(string destination) => local.GetSendTracing(destination);
            public ValueTask SendAsync(OutgoingMessage message, CancellationToken token) => local.SendAsync(message, token);
            public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token) => local.SendBatchAsync(messages, token);
            public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken token) => local.ApplyTopologyAsync(mode, token);
            public async Task StartAsync(CancellationToken token)
            {
                if (_direct is not null) return;
                _direct = new(source, new(), new(), [local], logs.CreateLogger<PostgreSqlDirectDispatcher>());
                _recovery = new(source, new() { RecoveryDelay = TimeSpan.Zero, SweepInterval = TimeSpan.FromMilliseconds(20) }, [local], logs.CreateLogger<PostgreSqlMaintenanceWorker>());
                // OrderingByKey holds the first handler while all sends commit. The worker
                // cancels a turn at half its lease, which must exceed the suite's 30-second deadline
                // so slow sends cannot roll back and redeliver the observed first row.
                _ordered = new(source, new() { Lanes = 1, PollInterval = TimeSpan.FromMilliseconds(20), LeaseDuration = TimeSpan.FromMinutes(2) }, [local], logs.CreateLogger<PostgreSqlOrderedDispatcher>());
                local.Dispatcher = _direct;
                await _direct.StartAsync(token);
                await _ordered.StartAsync(token);
                await _recovery.StartAsync(token);
            }
            public async Task StopAsync(CancellationToken token)
            {
                if (_direct is null) return;
                await _direct.StopAsync(token);
                await _ordered!.StopAsync(token);
                await _recovery!.StopAsync(token);
                _direct.Dispose(); _ordered.Dispose(); _recovery.Dispose();
                _direct = null; _ordered = null; _recovery = null;
                local.Dispatcher = null;
            }
            public async ValueTask DisposeAsync() => await StopAsync(default);
        }
    }
}
