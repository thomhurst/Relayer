using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Relayer.Sagas;
using Relayer.Sagas.EntityFrameworkCore;

namespace Relayer.Benchmarks;

/// <summary>Compares typed saga orchestration with the same native EF update inside an owned transaction.</summary>
/// <remarks>Requires RELAYER_BENCHMARK_POSTGRES. Query, inbox and outbox costs have separate benchmarks.</remarks>
[MemoryDiagnoser]
public class SagaBenchmarks
{
    private Context _database = null!;
    private State _state = null!;
    private Process _process = null!;
    private ServiceProvider _services = null!;
    private BenchmarkMessageContext _context = null!;
    private readonly Probe _message = new();
    private string _schema = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var connection = Environment.GetEnvironmentVariable("RELAYER_BENCHMARK_POSTGRES")
            ?? throw new InvalidOperationException("Set RELAYER_BENCHMARK_POSTGRES to a disposable PostgreSQL benchmark database.");
        _schema = "saga_bench_" + Guid.NewGuid().ToString("N");
        await using (var setup = new NpgsqlConnection(connection))
        {
            await setup.OpenAsync();
            await using var command = new NpgsqlCommand($"CREATE SCHEMA {_schema}; CREATE TABLE {_schema}.processes (\"Id\" uuid PRIMARY KEY, \"IsCompleted\" boolean NOT NULL, \"Count\" integer NOT NULL)", setup);
            await command.ExecuteNonQueryAsync();
        }
        var options = new DbContextOptionsBuilder<Context>().UseNpgsql(new NpgsqlConnectionStringBuilder(connection) { SearchPath = _schema }.ConnectionString).Options;
        _database = new Context(options);
        _state = new State { Id = Guid.NewGuid() };
        _database.Add(_state);
        await _database.SaveChangesAsync();
        _process = new Process(_database, _state);
        _services = new ServiceCollection().AddSingleton<SagaExecution<State, Probe>>().BuildServiceProvider();
        _context = new(_services);
    }

    [Benchmark(Baseline = true)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask NativeTransition()
    {
        await using var transaction = await _database.Database.BeginTransactionAsync().ConfigureAwait(false);
        _database.Entry(_state).Property(nameof(SagaData.IsCompleted)).IsModified = true;
        _state.Count++;
        await _database.SaveChangesAsync().ConfigureAwait(false);
        await transaction.CommitAsync().ConfigureAwait(false);
    }

    [Benchmark]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask SagaTransition()
    {
        await using var transaction = await _database.Database.BeginTransactionAsync().ConfigureAwait(false);
        await _process.HandleAsync(_message, _context, default).ConfigureAwait(false);
        await transaction.CommitAsync().ConfigureAwait(false);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _database.Database.OpenConnectionAsync();
        await using (var drop = new NpgsqlCommand($"DROP SCHEMA {_schema} CASCADE", (NpgsqlConnection)_database.Database.GetDbConnection()))
            await drop.ExecuteNonQueryAsync();
        await _database.DisposeAsync();
        await _services.DisposeAsync();
    }

    public sealed class State : EntityFrameworkCoreSagaData { public int Count { get; set; } }
    [Message("benchmark.saga")]
    public sealed class Probe;
    public sealed class Context(DbContextOptions<Context> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            var state = modelBuilder.Entity<State>().ToTable("processes");
            state.HasKey(s => s.Id);
            state.Property(s => s.Id).ValueGeneratedNever();
            state.Property(s => s.Version).IsRowVersion();
        }
    }
#pragma warning disable RLY003 // The benchmark constructs and invokes the saga directly.
    public sealed class Process(Context database, State state) : EntityFrameworkCoreSaga<Context, State, Probe>(database)
    {
#pragma warning restore RLY003
        protected override ValueTask<State?> LoadAsync(Probe message, CancellationToken cancellationToken) => new(state);
        protected override ValueTask TransitionAsync(State current, Probe message, MessageContext context, CancellationToken cancellationToken)
        {
            current.Count++;
            return default;
        }
    }
}

/// <summary>Isolates Relayer allocations using the same reusable, cross-thread native read fixture on both paths.</summary>
[MemoryDiagnoser]
public class SagaRuntimeBenchmarks
{
    private readonly RuntimeState _state = new() { Id = Guid.NewGuid() };
    private readonly SagaBenchmarks.Probe _message = new();
    private readonly Action _nativeContinuation = static () => { };
    private RuntimeProcess _process = null!;
    private ServiceProvider _services = null!;
    private BenchmarkMessageContext _context = null!;
    private Reader _reader = null!;

    [Params(false, true)]
    public bool Asynchronous { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _reader = new Reader(_state, Asynchronous);
        _process = new RuntimeProcess(_reader);
        _services = new ServiceCollection().AddSingleton<SagaExecution<RuntimeState, SagaBenchmarks.Probe>>().BuildServiceProvider();
        _context = new(_services);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _reader.Dispose();
        _services.Dispose();
    }

    [Benchmark(Baseline = true)]
    public int NativeTransition()
    {
        var pending = _reader.Read().ConfigureAwait(false).GetAwaiter();
        if (!pending.IsCompleted) pending.UnsafeOnCompleted(_nativeContinuation);
        _reader.Wait();
        return ++pending.GetResult()!.Count;
    }

    [Benchmark]
    public int SagaTransition()
    {
        var pending = _process.HandleAsync(_message, _context, default);
        _reader.Wait();
        pending.GetAwaiter().GetResult();
        return _state.Count;
    }

    public sealed class RuntimeState : SagaData { public int Count { get; set; } }
#pragma warning disable RLY003 // This benchmark invokes the saga directly without a consumer declaration.
    public sealed class RuntimeProcess(Reader reader) : Saga<RuntimeState, SagaBenchmarks.Probe>
    {
#pragma warning restore RLY003
        protected override void ValidateTransaction() { }
        protected override ValueTask<RuntimeState?> LoadAsync(SagaBenchmarks.Probe message, CancellationToken cancellationToken) => reader.Read();
        protected override ValueTask TransitionAsync(RuntimeState current, SagaBenchmarks.Probe message, MessageContext context, CancellationToken cancellationToken)
        {
            current.Count++;
            return default;
        }
        protected override ValueTask SaveAsync(RuntimeState current, bool isNew, CancellationToken cancellationToken) => default;
    }

    // A reusable native-I/O stand-in. Completion always follows continuation registration on a dedicated worker.
    // Blocking and signalling are fixture costs shared by both methods, not part of Relayer's implementation.
    public sealed class Reader : System.Threading.Tasks.Sources.IValueTaskSource<RuntimeState?>, IDisposable
    {
        private readonly RuntimeState _state;
        private readonly bool _asynchronous;
        private readonly AutoResetEvent _resume = new(false);
        private readonly ManualResetEventSlim _completed = new(false);
        private readonly Thread _worker;
        private System.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<RuntimeState?> _completion;
        private bool _stopped;

        public Reader(RuntimeState state, bool asynchronous)
        {
            _state = state;
            _asynchronous = asynchronous;
            _worker = new Thread(Run) { IsBackground = true };
            _worker.Start();
        }
        public ValueTask<RuntimeState?> Read()
        {
            if (!_asynchronous) return new(_state);
            _completed.Reset();
            _completion.Reset();
            return new(this, _completion.Version);
        }
        public void Wait() { if (_asynchronous) _completed.Wait(); }
        private void Run()
        {
            while (true)
            {
                _resume.WaitOne();
                if (Volatile.Read(ref _stopped)) return;
                _completion.SetResult(_state);
                _completed.Set();
            }
        }
        public RuntimeState? GetResult(short token) => _completion.GetResult(token);
        public System.Threading.Tasks.Sources.ValueTaskSourceStatus GetStatus(short token) => _completion.GetStatus(token);
        public void OnCompleted(Action<object?> continuation, object? state, short token, System.Threading.Tasks.Sources.ValueTaskSourceOnCompletedFlags flags)
        {
            _completion.OnCompleted(continuation, state, token, flags);
            _resume.Set();
        }
        public void Dispose()
        {
            Volatile.Write(ref _stopped, true);
            _resume.Set();
            _worker.Join();
            _resume.Dispose();
            _completed.Dispose();
        }
    }
}

internal sealed class BenchmarkMessageContext(IServiceProvider services) : MessageContext
{
    public override string MessageId => "018f0000-0000-7000-8000-000000000001";
    public override MessageType MessageType => new("benchmark.saga", 1);
    public override MessageHeaders Headers => MessageHeaders.Empty;
    public override int Attempt => 1;
    public override IServiceProvider Services => services;
    public override MessageFeatures Features { get; } = new();
    public override CancellationToken CancellationToken => default;
}
