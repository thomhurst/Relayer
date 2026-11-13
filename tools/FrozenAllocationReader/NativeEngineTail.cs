namespace Relayer.Diagnostics;

// BDN v0.15.8 Engine.Run calls GetExtraStats after the final timed iteration,
// before BenchmarkStop. This envelope also contains cleanup, logging and stats setup.
// v0.15.8 Engines/EngineEventSource.cs: IDs 3-14 are jitting/pilot/warmup/overhead;
// 15-16 are actual workload. Engine.GetExtraStats calls the workload directly,
// without IterationStart/Stop, so it cannot emit additional stage events.
internal sealed class NativeEngineTail
{
    public const string Provider = "BenchmarkDotNet.EngineEventSource";
    public const string Benchmark = "Relayer.Benchmarks.EndToEndBenchmarks.ConcurrentRoundTrip";
    public int Process { get; private set; }
    public int Thread { get; private set; }
    public double Start { get; private set; }
    public double Stop { get; private set; }
    public long Operations { get; private set; }
    public int Iterations { get; private set; }
    private bool prelude;
    private bool started;
    private bool stopped;
    private bool iterationOpen;
    private long pendingOperations;
    private double lastTime = double.NegativeInfinity;

    public void Add(int id, int process, int thread, double time, string? benchmark, long operations)
    {
        if (time < lastTime || stopped) throw new InvalidDataException("Unordered or additional native engine event.");
        lastTime = time;
        // EngineFactory can emit jitting iterations before Engine.Run starts the benchmark.
        if (!started && id is >= 3 and <= 6)
        {
            if (prelude && (process != Process || thread != Thread))
                throw new InvalidDataException("Native jitting events span processes or threads.");
            prelude = true;
            Process = process;
            Thread = thread;
            return;
        }
        if (id == 1)
        {
            if (started || benchmark != Benchmark || (prelude && (process != Process || thread != Thread))) throw new InvalidDataException("Expected one native ConcurrentRoundTrip benchmark.");
            started = true;
            Process = process;
            Thread = thread;
            return;
        }
        if (!started || process != Process || thread != Thread)
            throw new InvalidDataException("Native engine events must belong to one benchmark process and thread.");
        switch (id)
        {
            case 15:
                if (iterationOpen || operations <= 0 || operations % 8 != 0)
                    throw new InvalidDataException("Invalid native workload iteration start.");
                pendingOperations = operations;
                iterationOpen = true;
                break;
            case 16:
                if (!iterationOpen || operations != pendingOperations)
                    throw new InvalidDataException("Unmatched native workload iteration stop.");
                iterationOpen = false;
                Operations = operations;
                Start = time;
                Iterations++;
                break;
            case 2:
                if (benchmark != Benchmark || iterationOpen || Iterations == 0 || time <= Start)
                    throw new InvalidDataException("Incomplete native GC-stat tail envelope.");
                Stop = time;
                stopped = true;
                break;
            default:
                if (id < 3 || id > 14 || iterationOpen || Iterations != 0)
                    throw new InvalidDataException("Unexpected native engine stage.");
                break;
        }
    }

    public void Validate()
    {
        if (!stopped) throw new InvalidDataException("Missing native benchmark boundaries or final timed workload iteration.");
    }
}
