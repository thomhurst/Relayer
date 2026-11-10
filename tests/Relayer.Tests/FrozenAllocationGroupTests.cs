using Relayer.Diagnostics;

namespace Relayer.Tests;

public class FrozenAllocationGroupTests
{
    [Test]
    public async Task IdenticalStacksOnDifferentThreadsRemainSeparate()
    {
        var groups = new AllocationGroups();
        groups.Add(10, "Message", "Receive", 100);
        groups.Add(20, "Message", "Receive", 200);
        var values = groups.Values.OrderBy(g => g.ThreadId).ToArray();
        await Assert.That(values.Length).IsEqualTo(2);
        await Assert.That(values[0].SampledBytes).IsEqualTo(100L);
        await Assert.That(values[1].SampledBytes).IsEqualTo(200L);
    }

    [Test]
    public async Task GroupingPreservesSamplesAndWeightsWithoutMergingTypesOrStacks()
    {
        var groups = new AllocationGroups();
        groups.Add(10, "Message", "Receive", 100);
        groups.Add(10, "Message", "Receive", 200);
        groups.Add(10, "Other", "Receive", 300);
        groups.Add(10, "Message", "Send", 400);
        var values = groups.Values.ToArray();
        await Assert.That(values.Length).IsEqualTo(3);
        await Assert.That(values.Sum(g => g.Samples)).IsEqualTo(4L);
        await Assert.That(values.Sum(g => g.SampledBytes)).IsEqualTo(1000L);
        await Assert.That(values.Single(g => g.Type == "Message" && g.Stack == "Receive").Samples).IsEqualTo(2L);
    }
}
