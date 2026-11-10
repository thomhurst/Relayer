using System.Text.Json;
using Microsoft.Diagnostics.Tracing.Etlx;
using Microsoft.Diagnostics.Tracing.Parsers.Clr;

if (args.Length != 1) throw new ArgumentException("Expected one EventPipe trace.");
var etlx = TraceLog.CreateFromEventPipeDataFile(args[0], args[0] + ".etlx");
using var log = new TraceLog(etlx);
if (log.EventsLost != 0) throw new InvalidDataException($"Trace lost {log.EventsLost} events; recollect before interpreting owners.");
var starts = new List<(int Process, double Time)>();
var stops = new List<(int Process, double Time)>();
foreach (var ev in log.Events)
{
    if (ev.ProviderName != "Relayer-FrozenMeasurement") continue;
    if ((int)ev.ID == 1) starts.Add((ev.ProcessID, ev.TimeStampRelativeMSec));
    if ((int)ev.ID == 2) stops.Add((ev.ProcessID, ev.TimeStampRelativeMSec));
}
if (starts.Count != 1 || stops.Count != 1 || starts[0].Process != stops[0].Process || starts[0].Time >= stops[0].Time)
    throw new InvalidDataException("Trace must contain one ordered measurement start/stop pair from one process.");
var start = starts[0];
var stop = stops[0];
var groups = new Dictionary<string, AllocationGroup>();
long excludedSamples = 0;
long samplesWithoutStacks = 0;
foreach (var ev in log.Events)
{
    if (ev is not GCAllocationTickTraceData tick) continue;
    if (ev.ProcessID != start.Process || ev.TimeStampRelativeMSec <= start.Time || ev.TimeStampRelativeMSec >= stop.Time)
    {
        excludedSamples++;
        continue;
    }
    var frames = new List<string>();
    for (var frame = log.GetCallStackForEvent(ev); frame is not null; frame = frame.Caller)
        frames.Add(frame.CodeAddress.FullMethodName);
    if (frames.Count == 0) samplesWithoutStacks++;
    var stack = string.Join("\n", frames);
    // Preserve complete stacks. A shared prefix is not proof of a shared allocation owner.
    var key = tick.TypeName + "\n" + stack;
    if (!groups.TryGetValue(key, out var group))
        groups.Add(key, group = new AllocationGroup(tick.TypeName, stack));
    group.Samples++;
    group.SampledBytes += tick.AllocationAmount64;
}
Console.WriteLine(JsonSerializer.Serialize(new
{
    kind = "measurement", processId = start.Process, startMilliseconds = start.Time, stopMilliseconds = stop.Time,
    excludedSamples, samplesWithoutStacks,
    limits = "Allocation ticks are weighted samples, not exact object/byte counts. A tick can include bytes allocated before the boundary. Instrumentation changes scheduling. Do not subtract samples as an exact allocation delta."
}));
foreach (var group in groups.Values.OrderByDescending(g => g.SampledBytes))
    Console.WriteLine(JsonSerializer.Serialize(new { kind = "allocation", group.Type, group.Stack, group.Samples, group.SampledBytes }));
if (groups.Count == 0) throw new InvalidDataException("No allocation samples found inside the measured region.");

sealed class AllocationGroup(string type, string stack)
{
    public string Type { get; } = type;
    public string Stack { get; } = stack;
    public long Samples { get; set; }
    public long SampledBytes { get; set; }
}
