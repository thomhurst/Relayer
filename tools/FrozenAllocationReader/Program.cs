using System.Text.Json;
using Relayer.Diagnostics;
using Microsoft.Diagnostics.Tracing.Etlx;
using Microsoft.Diagnostics.Tracing.Parsers.Clr;

var wholeProcess = args.Length == 2 && args[1] == "--whole-process";
if (args.Length != 1 && !wholeProcess) throw new ArgumentException("Expected one EventPipe trace, optionally followed by --whole-process.");
var etlx = TraceLog.CreateFromEventPipeDataFile(args[0], args[0] + ".etlx");
using var log = new TraceLog(etlx);
if (log.EventsLost != 0) throw new InvalidDataException($"Trace lost {log.EventsLost} events; recollect before interpreting owners.");
// Keep runtime/module records from the entire capture: rundown can occur outside
// measurement markers. Payloads are recorded as supplied by TraceEvent, not inferred
// from the machine running this reader or from runtimeconfig requests.
var identityRecords = 0;
foreach (var ev in log.Events)
{
    if (!RuntimeIdentityScope.Includes(ev.ProviderName, ev.EventName)) continue;
    var payload = new Dictionary<string, string?>();
    foreach (var name in ev.PayloadNames) payload.Add(name, ev.PayloadByName(name)?.ToString());
    Console.WriteLine(JsonSerializer.Serialize(new
    {
        kind = "runtime-module-event", processId = ev.ProcessID, ev.ProviderName,
        ev.EventName, timeMilliseconds = ev.TimeStampRelativeMSec, payload
    }));
    identityRecords++;
}
Console.WriteLine(JsonSerializer.Serialize(new
{
    kind = "runtime-module-coverage", identityRecords,
    limits = "Only captured CLR runtime/module events. Missing fields remain absent; paths, versions and PDB signatures do not prove loaded binary bytes or historical execution identity."
}));
var observedFrames = new HashSet<(int Process, string Method, string? Module)>();
var starts = new List<(int Process, int Thread, double Time)>();
var stops = new List<(int Process, int Thread, double Time)>();
foreach (var ev in log.Events)
{
    if (ev.ProviderName != "Relayer-FrozenMeasurement") continue;
    if ((int)ev.ID == 1) starts.Add((ev.ProcessID, ev.ThreadID, ev.TimeStampRelativeMSec));
    if ((int)ev.ID == 2) stops.Add((ev.ProcessID, ev.ThreadID, ev.TimeStampRelativeMSec));
}
if (!wholeProcess && (starts.Count != 1 || stops.Count != 1 || starts[0].Process != stops[0].Process || starts[0].Time >= stops[0].Time))
    throw new InvalidDataException("Trace must contain one ordered measurement start/stop pair from one process.");
var start = wholeProcess ? default : starts[0];
var stop = wholeProcess ? default : stops[0];
// Keep separate processes distinct; a process may contain multiple allocating threads.
var processGroups = new Dictionary<int, AllocationGroups>();
long excludedSamples = 0;
long samplesWithoutStacks = 0;
long samplesWithUnnamedFrames = 0;
foreach (var ev in log.Events)
{
    if (ev is not GCAllocationTickTraceData tick) continue;
    if (!AllocationScope.Includes(wholeProcess, ev.ProcessID, ev.TimeStampRelativeMSec, start.Process, start.Time, stop.Time))
    {
        excludedSamples++;
        continue;
    }
    var frames = new List<string>();
    for (var frame = log.GetCallStackForEvent(ev); frame is not null; frame = frame.Caller)
    {
        frames.Add(frame.CodeAddress.FullMethodName);
        var address = frame.CodeAddress;
        if (observedFrames.Add((ev.ProcessID, address.FullMethodName, address.ModuleFilePath)))
            Console.WriteLine(JsonSerializer.Serialize(new
            {
                kind = "sampled-frame-module", processId = ev.ProcessID,
                method = address.FullMethodName, modulePath = address.ModuleFilePath,
                pdbSignature = address.ModuleFile?.PdbSignature,
                pdbAge = address.ModuleFile?.PdbAge
            }));
    }
    if (frames.Count == 0) samplesWithoutStacks++;
    if (frames.Any(string.IsNullOrWhiteSpace)) samplesWithUnnamedFrames++;
    var stack = string.Join("\n", frames);
    // Preserve complete stacks. A shared prefix is not proof of a shared allocation owner.
    if (!processGroups.TryGetValue(ev.ProcessID, out var groups))
        processGroups.Add(ev.ProcessID, groups = new AllocationGroups());
    groups.Add(ev.ThreadID, tick.TypeName, stack, tick.AllocationAmount64);
}
Console.WriteLine(JsonSerializer.Serialize(new
{
    kind = wholeProcess ? "whole-process" : "measurement",
    processId = wholeProcess ? (int?)null : start.Process,
    startMilliseconds = wholeProcess ? (double?)null : start.Time,
    stopMilliseconds = wholeProcess ? (double?)null : stop.Time,
    startThreadId = wholeProcess ? (int?)null : start.Thread, stopThreadId = wholeProcess ? (int?)null : stop.Thread,
    eventsLost = log.EventsLost, excludedSamples, samplesWithoutStacks, samplesWithUnnamedFrames,
    scope = wholeProcess ? "All allocation ticks in every captured process, including startup, pilot, warmup, overhead and GC-stat work; no measurement boundaries." : "Marker-delimited measurement",
    limits = "Allocation ticks are weighted samples, not exact object/byte counts. A tick can include bytes allocated before the boundary. Instrumentation changes scheduling. Do not subtract samples as an exact allocation delta."
}));
foreach (var process in processGroups.OrderBy(p => p.Key))
foreach (var group in process.Value.Values.OrderByDescending(g => g.SampledBytes))
    Console.WriteLine(JsonSerializer.Serialize(new { kind = "allocation", processId = process.Key, group.ThreadId, group.Type, group.Stack, group.Samples, group.SampledBytes }));
if (processGroups.Count == 0) throw new InvalidDataException("No allocation samples found in the selected scope.");
