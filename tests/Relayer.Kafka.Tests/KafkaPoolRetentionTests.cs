using Relayer.Tests.Shared;

namespace Relayer.Kafka.Tests;

public class KafkaPoolRetentionTests
{
    [Test]
    public async Task Burst_drops_overflow_and_bounds_pages_while_reusing_retained_state()
    {
        var pool = new KafkaSendPool();
        await BoundedTransportPoolAssertions.Burst(pool, () => pool.Rent(1), item => pool.Return((KafkaSendOperation)item), 64);
    }

    [Test]
    public async Task Concurrent_rentals_are_exclusive()
    {
        var pool = new KafkaSendPool();
        await BoundedTransportPoolAssertions.Exclusive(() => pool.Rent(1), item => pool.Return((KafkaSendOperation)item));
    }
}
