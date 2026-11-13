using System.Runtime.CompilerServices;

var region = NativeAllocationRegion.Load() ?? throw new InvalidOperationException("Profiler path is required.");
var count = int.Parse(args[0], System.Globalization.CultureInfo.InvariantCulture);
var values = new object[count];
// JIT all fixture allocation code before the marker, without counting warmup objects.
Fill(new object[10]);
var start = new ManualResetEventSlim();
var ready = new CountdownEvent(4);
var workers = new Thread[4];
for (var index = 0; index < workers.Length; index++)
{
    var from = count * index / workers.Length;
    var to = count * (index + 1) / workers.Length;
    workers[index] = new Thread(() =>
    {
        ready.Signal();
        start.Wait();
        FillRange(values, from, to);
    });
    workers[index].Start();
}
ready.Wait();
region.Begin();
start.Set();
foreach (var worker in workers) worker.Join();
region.End();
GC.KeepAlive(values);

[MethodImpl(MethodImplOptions.NoInlining)]
static void Fill(object[] values)
{
    FillRange(values, 0, values.Length);
}

[MethodImpl(MethodImplOptions.NoInlining)]
static void FillRange(object[] values, int from, int to)
{
    for (var i = from; i < to; i++) values[i] = new FixtureTypes.Box<FixtureMessage>(new FixtureMessage(i));
}

internal sealed class FixtureMessage(int value)
{
    internal int Value { get; } = value;
}

internal static class FixtureTypes
{
    internal sealed class Box<T>(T value)
    {
        internal T Value { get; } = value;
    }
}
