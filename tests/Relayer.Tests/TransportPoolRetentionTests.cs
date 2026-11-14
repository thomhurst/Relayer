using Relayer.AmazonSqs;
using Relayer.AzureServiceBus;
using Relayer.RabbitMQ;
using Relayer.Tests.Shared;

namespace Relayer.Tests;

public class TransportPoolRetentionTests
{
    [Test]
    [Arguments("service-bus")]
    [Arguments("rabbitmq")]
    [Arguments("sqs")]
    [Arguments("sns")]
    public async Task Burst_drops_overflow_and_bounds_pages_while_reusing_retained_state(string transport)
    {
        var (pool, rent, giveBack, pageSize) = Create(transport);
        await BoundedTransportPoolAssertions.Burst(pool, rent, giveBack, pageSize);
    }

    [Test]
    [Arguments("service-bus")]
    [Arguments("rabbitmq")]
    [Arguments("sqs")]
    [Arguments("sns")]
    public async Task Concurrent_rentals_are_exclusive(string transport)
    {
        var (_, rent, giveBack, _) = Create(transport);
        await BoundedTransportPoolAssertions.Exclusive(rent, giveBack);
    }

    private static (object Pool, Func<object> Rent, Action<object> Return, int PageSize) Create(string transport)
    {
        switch (transport)
        {
            case "service-bus":
                var serviceBus = new ServiceBusPool<ServiceBusSendState>(static () => new());
                return (serviceBus, () => serviceBus.Rent(), item => serviceBus.Return((ServiceBusSendState)item), 64);
            case "rabbitmq":
                var rabbit = new RabbitMqSendPool();
                return (rabbit, () => rabbit.Rent(), item => rabbit.Return((RabbitMqSendOperation)item), 64);
            case "sqs":
                var sqs = new SqsOperationPool();
                return (sqs, () => sqs.Rent(), item => sqs.Return((SqsOperationState)item), 32);
            default:
                var sns = new SnsOperationPool();
                return (sns, () => sns.Rent(), item => sns.Return((SnsOperationState)item), 32);
        }
    }
}
