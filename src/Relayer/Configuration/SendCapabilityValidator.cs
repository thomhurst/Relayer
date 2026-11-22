using Relayer.Transports;

namespace Relayer;

internal static class SendCapabilityValidator
{
    internal static string? GetNativeCallbackError(ISendRouteDefinition route, Type transportOptionsType)
    {
        return route.NativeTransportOptionsType is { } native && native != transportOptionsType
            ? $"the route's native callback is for a different transport ({native.Name}); declare it only on routes of that transport."
            : null;
    }

    internal static bool NeedsOutbox(SendRouteOptions options, TransportCapabilities capabilities)
    {
        return options.TransactionRequirement == SendTransactionRequirement.Outbox ||
            (options.TransactionRequirement != SendTransactionRequirement.Native && NeedsSchedulingFallback(options, capabilities));
    }

    internal static void Validate(SendRouteOptions options, TransportCapabilities capabilities, bool ordered,
        OutboxCapabilities.Snapshot? executor, string location, List<string> failures)
    {
        if (options.TransactionRequirement == SendTransactionRequirement.Native && !capabilities.Transactions)
        {
            failures.Add($"{location}: native Transactions are unavailable. Use an endpoint with native transactions, or explicitly select TransactionRequirement.Outbox for database durability; the guarantees are different.");
        }

        if (options.TransactionRequirement == SendTransactionRequirement.Outbox && executor?.Transactions != true)
        {
            failures.Add($"{location}: TransactionRequirement.Outbox requires a registered IOutboxEnlistment transaction executor. Register an outbox integration and send within its active transaction.");
        }

        if (NeedsSchedulingFallback(options, capabilities) &&
            (options.TransactionRequirement == SendTransactionRequirement.Native ||
             executor is not { Transactions: true } ||
             !(ordered ? executor.OrderedScheduling : executor.Scheduling)))
        {
            failures.Add($"{location}: RequiredDeliveryDelay {options.RequiredDeliveryDelay} has no compatible executor for the selected transaction requirement and native MaxDeliveryDelay. Shorten the delay, select native scheduling with a sufficient limit, or register an outbox integration and a matching due-row dispatch worker. Ordered routes require native ordered dispatch.");
        }
    }

    private static bool NeedsSchedulingFallback(SendRouteOptions options, TransportCapabilities capabilities)
    {
        return options.RequiredDeliveryDelay is { } delay &&
            (options.TransactionRequirement == SendTransactionRequirement.Outbox ||
             capabilities.MaxDeliveryDelay is not { } maximum || delay > maximum);
    }
}
