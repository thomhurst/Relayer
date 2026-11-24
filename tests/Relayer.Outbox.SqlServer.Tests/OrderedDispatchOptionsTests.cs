namespace Relayer.Outbox.SqlServer.Tests;

public sealed class OrderedDispatchOptionsTests
{
    [Test]
    public async Task Poll_interval_must_be_at_least_one_millisecond()
    {
        new SqlServerOrderedDispatchOptions { PollInterval = TimeSpan.FromMilliseconds(1) }.Validate();

        var options = new SqlServerOrderedDispatchOptions { PollInterval = TimeSpan.FromTicks(1) };
        var error = await Assert.That(() => options.Validate()).Throws<ArgumentOutOfRangeException>();
        await Assert.That(error!.ParamName).IsEqualTo(nameof(options.PollInterval));
    }
}
