using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Relayer.Transports;

namespace Relayer.Outbox.PostgreSql;

// Only this scheduler executes catalog stores. A turn uses existing operations serially, each of
// which holds at most one native connection. The fixed parallelism is the aggregate connection budget.
internal sealed partial class PostgreSqlTenantDispatcher : BackgroundService
{
    private readonly ITenantSource _catalog;
    private readonly PostgreSqlTenantDispatchOptions _options;
    private readonly ITransport[] _transports;
    private readonly ILoggerFactory _logs;
    private readonly RelayerInstrumentation? _instrumentation;
    private readonly TimeProvider? _orderedTimeProvider;
    private readonly TimeProvider? _recoveryTimeProvider;
    private readonly ILogger _logger;
    private readonly object _gate = new();
    private readonly Dictionary<string, Store> _stores = new(StringComparer.Ordinal);
    private bool _stopped;

    public PostgreSqlTenantDispatcher(ITenantSource catalog, PostgreSqlTenantDispatchOptions options,
        IEnumerable<ITransport> transports, ILoggerFactory logs, RelayerInstrumentation? instrumentation = null, TimeProvider? orderedTimeProvider = null, TimeProvider? recoveryTimeProvider = null)
    {
        _catalog = catalog;
        _options = options;
        options.Validate();
        _transports = transports.ToArray();
        if (_transports.Length == 0)
            throw new InvalidOperationException("Tenant outbox dispatch requires at least one named ITransport adapter.");
        _logs = logs;
        _instrumentation = instrumentation;
        _orderedTimeProvider = orderedTimeProvider;
        _recoveryTimeProvider = recoveryTimeProvider;
        _logger = logs.CreateLogger<PostgreSqlTenantDispatcher>();
    }

    internal PostgreSqlDirectDispatcher GetDispatcher(string tenantId) => GetDispatcher(tenantId, out _);

    internal PostgreSqlDirectDispatcher GetDispatcher(string tenantId, out PostgreSqlSchedulingRegistration[] scheduling)
    {
        var snapshot = ValidateSnapshot();
        lock (_gate)
        {
            if (_stopped) throw new InvalidOperationException("The tenant dispatcher has stopped.");
            foreach (var tenant in snapshot)
                if (tenant.TenantId == tenantId)
                {
                    if (_stores.TryGetValue(tenantId, out var current))
                    {
                        if (!SameStore(current.Entry, tenant))
                            throw new InvalidOperationException("The tenant store changed. Wait for the next catalog pass before creating a new scope.");
                        scheduling = current.Scheduling;
                        return current.Direct;
                    }
                    var store = Create(tenant);
                    _stores.Add(tenantId, store);
                    scheduling = store.Scheduling;
                    return store.Direct;
                }
        }
        throw new InvalidOperationException($"Tenant dispatch store '{tenantId}' is not in the authorized catalog.");
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var entry in ValidateSnapshot())
        {
            ValidateEntry(entry);
            await PostgreSqlSentSchema.ValidateAsync(entry.DataSource, entry.Options.RetainSentMessages,
                entry.Options.Schema, entry.Options.TablePrefix, cancellationToken).ConfigureAwait(false);
        }
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try { await PassAsync(stoppingToken).ConfigureAwait(false); }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
                catch (Exception error) { Failed(_logger, "catalog", error); }
                await Task.Delay(_options.PollInterval, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally { await ReleaseAllAsync().ConfigureAwait(false); }
    }

    // Invoked serially by the one hosted loop. Catalog refresh is finite, never a per-tenant task loop.
    internal async Task PassAsync(CancellationToken cancellationToken)
    {
        var entries = ValidateSnapshot();
        List<Store> active = [];
        List<Store> removed = [];
        var indexed = new Dictionary<string, PostgreSqlTenantStore>(StringComparer.Ordinal);
        foreach (var entry in entries) indexed.Add(entry.TenantId, entry);
        lock (_gate)
        {
            foreach (var existing in _stores.Values)
            {
                if (!indexed.TryGetValue(existing.Entry.TenantId, out var entry) || !SameStore(entry, existing.Entry))
                { existing.Retired = true; removed.Add(existing); }
            }
            foreach (var old in removed) _stores.Remove(old.Entry.TenantId);
            foreach (var entry in entries)
            {
                try
                {
                    if (!_stores.TryGetValue(entry.TenantId, out var store))
                        _stores.Add(entry.TenantId, store = Create(entry));
                    active.Add(store);
                }
                catch (Exception error) { Failed(_logger, entry.TenantId, error); }
            }
        }
        // Retirements use the very same budget as normal work, including their lease-release connections.
        active.AddRange(removed);
        try
        {
            await Parallel.ForEachAsync(active, new ParallelOptions
            {
                MaxDegreeOfParallelism = _options.MaxConnections, CancellationToken = cancellationToken
            }, async (store, token) =>
            {
                if (store.Retired) { await ReleaseAsync(store).ConfigureAwait(false); return; }
                using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
                var turnStarted = Stopwatch.GetTimestamp();
                deadline.CancelAfter(_options.TurnTimeout);
                try
                {
                    // Catalog entries can be added after startup; successful validation is cached.
                    await store.Direct.ValidateSchemaAsync(deadline.Token).ConfigureAwait(false);
                    for (var visited = 0; visited < 4; visited++)
                    {
                        deadline.Token.ThrowIfCancellationRequested();
                        var phase = store.NextPhase;
                        // Persist progress before awaiting so even an exhausted turn rotates fairly.
                        store.NextPhase = (phase + 1) % 4;
                        try
                        {
                            switch (phase)
                            {
                                case 0:
                                    await store.Direct.DispatchQueuedAsync(deadline.Token).ConfigureAwait(false);
                                    break;
                                case 1:
                                    await store.Maintenance.SweepAsync(_options.TurnTimeout - Stopwatch.GetElapsedTime(turnStarted), deadline.Token).ConfigureAwait(false);
                                    break;
                                case 2:
                                    await store.Ordered.DispatchPassAsync(_options.TurnTimeout - Stopwatch.GetElapsedTime(turnStarted), deadline.Token).ConfigureAwait(false);
                                    break;
                                case 3 when DateTimeOffset.UtcNow >= store.NextCleanup:
                                    await store.Maintenance.CleanupAsync(deadline.Token).ConfigureAwait(false);
                                    store.NextCleanup = DateTimeOffset.UtcNow + _options.CleanupInterval;
                                    break;
                            }
                        }
                        catch (OperationCanceledException) when (deadline.IsCancellationRequested) { throw; }
                        catch (Exception error) { Failed(_logger, store.Entry.TenantId, error); }
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested) { throw; }
                catch (OperationCanceledException) when (deadline.IsCancellationRequested) { }
                catch (Exception error) { Failed(_logger, store.Entry.TenantId, error); }
            }).ConfigureAwait(false);
        }
        finally
        {
            // Cancellation can stop admission before removed entries execute. Dispose their queues;
            // their un-released native leases remain fenced and expire normally.
            foreach (var store in removed) store.Dispose();
        }
    }

    internal async Task ReleaseAllAsync()
    {
        Store[] stores;
        lock (_gate)
        {
            _stopped = true;
            stores = _stores.Values.ToArray();
            _stores.Clear();
        }
        using var shutdown = new CancellationTokenSource(_options.TurnTimeout);
        await Parallel.ForEachAsync(stores, new ParallelOptions { MaxDegreeOfParallelism = _options.MaxConnections },
            async (store, _) => await ReleaseAsync(store, shutdown.Token).ConfigureAwait(false)).ConfigureAwait(false);
    }

    private async ValueTask ReleaseAsync(Store store, CancellationToken cancellationToken = default)
    {
        store.Direct.Retire();
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(_options.TurnTimeout);
        try
        {
            await store.Maintenance.ReleaseAsync(deadline.Token).ConfigureAwait(false);
            await store.Ordered.ReleaseAsync(deadline.Token).ConfigureAwait(false);
        }
        catch (Exception error) { Failed(_logger, store.Entry.TenantId, error); }
        finally { store.Dispose(); }
    }

    private IReadOnlyList<PostgreSqlTenantStore> ValidateSnapshot()
    {
        var entries = _catalog.GetTenants() ?? throw new InvalidOperationException("ITenantSource returned a null snapshot.");
        var ids = new HashSet<string>(StringComparer.Ordinal);
        var physical = new HashSet<(NpgsqlDataSource, string, string)>();
        foreach (var entry in entries)
        {
            ArgumentNullException.ThrowIfNull(entry);
            ArgumentException.ThrowIfNullOrWhiteSpace(entry.TenantId);
            ArgumentNullException.ThrowIfNull(entry.DataSource);
            ArgumentNullException.ThrowIfNull(entry.Options);
            if (!ids.Add(entry.TenantId)) throw new InvalidOperationException("Tenant catalog IDs must be unique.");
            if (!physical.Add((entry.DataSource, entry.Options.Schema ?? "public", entry.Options.TablePrefix)))
                throw new InvalidOperationException("Tenant catalog stores must have isolated tables and leases.");
        }
        return entries;
    }

    private void ValidateEntry(PostgreSqlTenantStore entry)
    {
        if (new NpgsqlConnectionStringBuilder(entry.DataSource.ConnectionString).Pooling)
            throw new InvalidOperationException("Tenant dispatch requires native Pooling=false data sources to bound physical connections across stores. Use separate pooled sources for business transactions.");
        _ = OrderedOptions(entry.Options).Validate();
        _ = _options.Maintenance(entry.Options).Validate();
    }

    private Store Create(PostgreSqlTenantStore entry)
    {
        ValidateEntry(entry);
        return new(entry,
        new(entry.DataSource, entry.Options, new() { QueueCapacity = _options.QueueCapacity, BatchSize = _options.BatchSize,
            ClaimDuration = entry.Options.LeaseDuration }, _transports, _logs.CreateLogger<PostgreSqlDirectDispatcher>(), instrumentation: _instrumentation),
        new(entry.DataSource, _options.Maintenance(entry.Options), _transports, _logs.CreateLogger<PostgreSqlMaintenanceWorker>(),
            _logs.CreateLogger<PostgreSqlDirectDispatcher>(), _instrumentation, _recoveryTimeProvider),
        new(entry.DataSource, OrderedOptions(entry.Options), _transports, _logs.CreateLogger<PostgreSqlOrderedDispatcher>(), _orderedTimeProvider, _instrumentation));
    }

    private PostgreSqlOrderedDispatchOptions OrderedOptions(PostgreSqlOutboxOptions store) => new()
    {
        Schema = store.Schema, TablePrefix = store.TablePrefix, Lanes = store.Lanes,
        LeaseDuration = store.LeaseDuration, MaxDispatchAttempts = store.MaxDispatchAttempts, RetainSentMessages = store.RetainSentMessages,
        BatchSize = _options.BatchSize, PollInterval = store.LeaseDuration / 4
    };

    private static bool SameStore(PostgreSqlTenantStore left, PostgreSqlTenantStore right) =>
        ReferenceEquals(left.DataSource, right.DataSource) && (left.Options.Schema ?? "public") == (right.Options.Schema ?? "public") &&
        left.Options.TablePrefix == right.Options.TablePrefix && left.Options.Lanes == right.Options.Lanes &&
        left.Options.LeaseDuration == right.Options.LeaseDuration && left.Options.MaxDispatchAttempts == right.Options.MaxDispatchAttempts && left.Options.RetainSentMessages == right.Options.RetainSentMessages;

    private sealed class Store(PostgreSqlTenantStore entry, PostgreSqlDirectDispatcher direct,
        PostgreSqlMaintenanceWorker maintenance, PostgreSqlOrderedDispatcher ordered) : IDisposable
    {
        internal PostgreSqlTenantStore Entry { get; } = entry;
        internal PostgreSqlSchedulingRegistration[] Scheduling { get; } =
            [new(entry.Options.Schema, entry.Options.TablePrefix), new(entry.Options.Schema, entry.Options.TablePrefix, entry.Options.Lanes)];
        internal PostgreSqlDirectDispatcher Direct { get; } = direct;
        internal PostgreSqlMaintenanceWorker Maintenance { get; } = maintenance;
        internal PostgreSqlOrderedDispatcher Ordered { get; } = ordered;
        internal DateTimeOffset NextCleanup { get; set; }
        internal int NextPhase { get; set; }
        internal bool Retired { get; set; }
        private bool _disposed;
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            Direct.Retire(); Direct.Dispose(); Maintenance.Dispose(); Ordered.Dispose();
        }
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "PostgreSQL tenant dispatch failed for {Tenant}; durable work remains available for a later pass.")]
    private static partial void Failed(ILogger logger, string tenant, Exception error);
}
