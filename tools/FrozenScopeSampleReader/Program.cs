using System.Text.Json;
using Microsoft.Diagnostics.Tracing.Etlx;
using Microsoft.Diagnostics.Tracing.EventPipe;
using Relayer.Diagnostics;

if (args.Length != 1) throw new ArgumentException("Expected one original scope EventPipe trace.");
// Reuse TraceEvent's native file conversion and symbol/stack association. Raw
// .nettrace remains authoritative; this export does not implement tier attribution.
var etlx = TraceLog.CreateFromEventPipeDataFile(args[0], args[0] + ".etlx");
using var log = new TraceLog(etlx);
if (log.EventsLost != 0) throw new InvalidDataException($"Trace lost {log.EventsLost} events.");
var benchmarkStarts = 0;
var benchmarkStops = 0;
var workloadStarts = 0;
var workloadStops = 0;
var methodRecords = 0;
long samples = 0;
long samplesWithoutStacks = 0;
long unnamedFrames = 0;
foreach (var ev in log.Events)
{
    if (ev.ProviderName == "BenchmarkDotNet.EngineEventSource" ||
        RuntimeIdentityScope.Includes(ev.ProviderName, ev.EventName) ||
        ((ev.ProviderName == "Microsoft-Windows-DotNETRuntime" || ev.ProviderName == "Microsoft-Windows-DotNETRuntimeRundown") &&
         ev.EventName.StartsWith("Method/", StringComparison.Ordinal)))
    {
        var payload = new Dictionary<string, string?>();
        foreach (var name in ev.PayloadNames) payload.Add(name, ev.PayloadByName(name)?.ToString());
        Console.WriteLine(JsonSerializer.Serialize(new
        {
            kind = "native-event", id = (int)ev.ID, processId = ev.ProcessID,
            threadId = ev.ThreadID, timeMilliseconds = ev.TimeStampRelativeMSec,
            ev.ProviderName, ev.EventName, payload
        }));
        if (ev.ProviderName == "BenchmarkDotNet.EngineEventSource")
        {
            switch ((int)ev.ID)
            {
                case 1: benchmarkStarts++; break;
                case 2: benchmarkStops++; break;
                case 15: workloadStarts++; break;
                case 16: workloadStops++; break;
            }
        }
        else if (ev.EventName.StartsWith("Method/", StringComparison.Ordinal)) methodRecords++;
    }
    if (ev is not ClrThreadSampleTraceData sample) continue;
    // Keep all phases and threads. A later analysis must select the original
    // benchmark thread's complete WorkloadActual pairs, not startup or GC stats.
    var frames = new List<object>();
    for (var frame = log.GetCallStackForEvent(ev); frame is not null; frame = frame.Caller)
    {
        var address = frame.CodeAddress;
        if (string.IsNullOrWhiteSpace(address.FullMethodName)) unnamedFrames++;
        frames.Add(new { address = address.Address, method = address.FullMethodName,
            modulePath = address.ModuleFilePath });
    }
    samples++;
    if (frames.Count == 0) samplesWithoutStacks++;
    Console.WriteLine(JsonSerializer.Serialize(new
    {
        kind = "thread-sample", processId = ev.ProcessID, threadId = ev.ThreadID,
        timeMilliseconds = ev.TimeStampRelativeMSec, sampleType = sample.Type.ToString(), frames
    }));
}
Console.WriteLine(JsonSerializer.Serialize(new
{
    kind = "coverage", eventsLost = log.EventsLost, benchmarkStarts, benchmarkStops,
    workloadStarts, workloadStops, methodRecords, samples, samplesWithoutStacks, unnamedFrames,
    limits = "All phases and threads retained. Samples are statistical thread-time observations, not CPU cycles or proof which tier ran throughout measurement. Missing stacks/frames remain unresolved; method/rundown records do not prove execution. Validate marker pairing, benchmark identity and same-process address ranges before attribution. Instrumented timings/allocations are excluded."
}));
if (benchmarkStarts != 1 || benchmarkStops != 1 || workloadStarts == 0 || workloadStarts != workloadStops ||
    methodRecords == 0 || samples == 0 || samplesWithoutStacks == samples)
    throw new InvalidDataException("Missing native scope benchmark, method or sample coverage; retain all evidence without interpretation.");
