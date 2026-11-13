using Relayer.Diagnostics;

namespace Relayer.Tests;

public class FrozenNativeEngineTailTests
{
    private static NativeEngineTail Started()
    {
        var tail = new NativeEngineTail();
        tail.Add(3, 7, 9, 0.1, null, 16);
        tail.Add(4, 7, 9, 0.2, null, 16);
        tail.Add(5, 7, 9, 0.3, null, 16);
        tail.Add(6, 7, 9, 0.4, null, 16);
        tail.Add(1, 7, 9, 1, NativeEngineTail.Benchmark, 0);
        return tail;
    }

    [Test]
    public async Task SelectsLastCompleteActualIterationAndNativeDenominator()
    {
        var tail = Started();
        tail.Add(7, 7, 9, 2, null, 16);
        tail.Add(8, 7, 9, 3, null, 16);
        tail.Add(15, 7, 9, 4, null, 32768);
        tail.Add(16, 7, 9, 5, null, 32768);
        tail.Add(15, 7, 9, 6, null, 32768);
        tail.Add(16, 7, 9, 7, null, 32768);
        tail.Add(2, 7, 9, 9, NativeEngineTail.Benchmark, 0);
        tail.Validate();
        await Assert.That(tail.Start).IsEqualTo(7);
        await Assert.That(tail.Stop).IsEqualTo(9);
        await Assert.That(tail.Operations).IsEqualTo(32768);
        await Assert.That(tail.Iterations).IsEqualTo(2);
        await Assert.That(AllocationScope.Includes(false, 7, 8, tail.Process, tail.Start, tail.Stop)).IsTrue();
        await Assert.That(AllocationScope.Includes(false, 8, 8, tail.Process, tail.Start, tail.Stop)).IsFalse();
    }

    [Test]
    [Arguments("missing-stop")]
    [Arguments("no-actual")]
    [Arguments("unmatched-stop")]
    [Arguments("denominator")]
    [Arguments("process")]
    [Arguments("thread")]
    [Arguments("time")]
    [Arguments("second-benchmark")]
    [Arguments("stage-after-actual")]
    [Arguments("wrong-benchmark")]
    [Arguments("partial-iteration")]
    [Arguments("zero-operations")]
    [Arguments("non-message-multiple")]
    public async Task RejectsIncompleteOrAmbiguousNativeEvents(string invalid)
    {
        await Assert.ThrowsAsync<InvalidDataException>(() =>
        {
            var tail = Started();
            if (invalid == "no-actual") tail.Add(2, 7, 9, 2, NativeEngineTail.Benchmark, 0);
            if (invalid == "unmatched-stop") tail.Add(16, 7, 9, 2, null, 16);
            if (invalid == "second-benchmark") tail.Add(1, 7, 9, 2, NativeEngineTail.Benchmark, 0);
            tail.Add(15, 7, 9, 3, null, invalid == "zero-operations" ? 0 : invalid == "non-message-multiple" ? 9 : 16);
            if (invalid != "partial-iteration")
                tail.Add(16, invalid == "process" ? 8 : 7, invalid == "thread" ? 10 : 9,
                    invalid == "time" ? 2 : 4, null, invalid == "denominator" ? 24 : 16);
            if (invalid == "stage-after-actual") tail.Add(7, 7, 9, 5, null, 16);
            if (invalid != "missing-stop")
                tail.Add(2, 7, 9, 6, invalid == "wrong-benchmark" ? "Other" : NativeEngineTail.Benchmark, 0);
            tail.Validate();
            return Task.CompletedTask;
        });
    }
}
