var builder = DistributedApplication.CreateBuilder(args);

var kafka = builder.AddKafka("kafka");
var serviceBus = builder.AddAzureServiceBus("servicebus").RunAsEmulator();
serviceBus.AddServiceBusQueue("orders");

builder.AddDotnetProject("api", "../RelayerSample.Api/RelayerSample.Api.csproj")
    .WithReference(kafka)
    .WithReference(serviceBus)
    .WaitFor(kafka)
    .WaitFor(serviceBus)
    .WithHttpEndpoint(env: "ASPNETCORE_HTTP_PORTS")
    .WithHttpHealthCheck("/health");

builder.Build().Run();
