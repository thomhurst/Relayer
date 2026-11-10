# Azure Service Bus web API

Create a non-session queue named `sample-orders` in an Azure Service Bus namespace or the Service Bus emulator. Use a connection string with Send and Listen rights. The sample validates existing topology at startup and never creates or alters the queue. Emulator users must supply its `UseDevelopmentEmulator=true` connection string and configure the queue in the emulator's configuration before starting it.

Set configuration in your shell (Bash/zsh example):

```shell
export ConnectionStrings__ServiceBus='<namespace or emulator connection string>'
dotnet run -c Release --project samples/ServiceBusWebApi -- --urls http://localhost:5082
```

From another terminal:

```shell
curl -i -X POST http://localhost:5082/orders/order-1
```

Expect HTTP 202 and `Received order order-1` in the application log. Ctrl+C stops the processor and drains callbacks. A successful HTTP response means the native send completed; it does not wait for the handler. The handler only logs: a production handler must finish durable work before returning, and tolerate at-least-once delivery. There is no database outbox in this sample.

PowerShell environment syntax is `$env:ConnectionStrings__ServiceBus = '<connection string>'`. Keep credentials outside source control. See [Service Bus](https://relayer.tomlonghurst.dev/docs/transports/service-bus) for native options, provisioning, sessions and settlement.
