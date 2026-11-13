using Relayer;
using Relayer.Aspire;
using Relayer.AzureServiceBus;
using Relayer.HealthChecks;
using Relayer.Kafka;
using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    // Aspire provisions the Service Bus queue; Relayer only validates/uses native entities.
    relayer.Kafka("events", kafka =>
    {
        kafka.UseAspireConnection(builder.Configuration, "kafka");
        kafka.TopologyMode = TopologyMode.None; // Skip validation for this local broker with automatic topic creation.
        kafka.Send<KafkaOrder>("orders");
    });
    relayer.ServiceBus("commands", bus =>
    {
        bus.UseAspireConnection(builder.Configuration, "servicebus");
        bus.Options.Configure(options => options.Queues.Add(new("orders")));
        bus.Send<ServiceBusOrder>("orders");
    });
});
builder.Services.AddRelayerHealthChecks();
builder.Services.AddRelayerKafkaHealthProviders("events");
builder.Services.AddRelayerServiceBusHealthProviders("commands");
builder.Services.AddOpenTelemetry().AddRelayerInstrumentation()
    .WithTracing(tracing => tracing.AddAspNetCoreInstrumentation(options =>
        options.Filter = context => !context.Request.Path.StartsWithSegments("/health")));
// Aspire injects OTLP endpoint/authentication. Existing ServiceDefaults can own this instead.
if (!string.IsNullOrWhiteSpace(builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"]))
{
    builder.Services.AddOpenTelemetry().UseOtlpExporter();
    builder.Logging.AddOpenTelemetry(logging => logging.IncludeFormattedMessage = true);
}

var app = builder.Build();
app.MapHealthChecks("/health");
app.MapPost("/kafka", async (ISender sender, CancellationToken cancellationToken) =>
{
    await sender.SendAsync(new KafkaOrder(Guid.NewGuid().ToString()), cancellationToken: cancellationToken);
    return Results.Accepted();
});
app.MapPost("/servicebus", async (ISender sender, CancellationToken cancellationToken) =>
{
    await sender.SendAsync(new ServiceBusOrder(Guid.NewGuid().ToString()), cancellationToken: cancellationToken);
    return Results.Accepted();
});
app.Run();

[Message("sample.kafka-order")]
public sealed record KafkaOrder(string Id);
[Message("sample.servicebus-order")]
public sealed record ServiceBusOrder(string Id);
