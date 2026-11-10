# In-memory modular monolith

Checkout and billing are separate classes connected by an explicitly registered message route and handler. No broker or database is needed.

```shell
dotnet run -c Release --project samples/ModularMonolith
```

Expected output includes `Invoice created for order-1 (customer-1)`, then the process exits successfully. It waits for handler completion with a 30-second deadline; sending alone only confirms queue admission. The singleton billing module holds the completion signal for this demonstration; handlers remain scoped.

The queue, completion signal and printed invoice are process-local. Restart loses them. For durable business data, use a database and the outbox/inbox integrations rather than treating this as a durable queue. See the [in-memory guide](https://relayer.tomlonghurst.dev/docs/transports/in-memory).
