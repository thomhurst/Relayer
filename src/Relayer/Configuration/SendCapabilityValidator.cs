using Relayer.Transports;

namespace Relayer;

internal static class SendCapabilityValidator
{
    internal static void Validate(SendRouteOptions options, TransportCapabilities capabilities, bool ordered,
        OutboxCapabilities outbox, string location, List<string> failures)
    {
        if (options.TransactionRequirement == SendTransactionRequirement.Native && !capabilities.Transactions)
            failures.Add($"{location}: native Transactions are unavailable. Use an endpoint with native transactions, or explicitly select TransactionRequirement.Outbox for database durability; the guarantees are different.");
        if (options.TransactionRequirement == SendTransactionRequirement.Outbox && !outbox.Get().Transactions)
            failures.Add($"{location}: TransactionRequirement.Outbox requires a registered IOutboxEnlistment transaction executor. Register an outbox integration and send within its active transaction.");
        if (options.RequiredDeliveryDelay is { } delay &&
            (options.TransactionRequirement == SendTransactionRequirement.Outbox ||
             capabilities.MaxDeliveryDelay is not { } maximum || delay > maximum))
        {
            var executor = outbox.Get();
            if (options.TransactionRequirement == SendTransactionRequirement.Native || !executor.Transactions || !(ordered ? executor.OrderedScheduling : executor.Scheduling))
                failures.Add($"{location}: RequiredDeliveryDelay {delay} has no compatible executor for the selected transaction requirement and native MaxDeliveryDelay. Shorten the delay, select native scheduling with a sufficient limit, or register an outbox integration and a matching due-row dispatch worker. Ordered routes require native ordered dispatch.");
        }
    }
}
