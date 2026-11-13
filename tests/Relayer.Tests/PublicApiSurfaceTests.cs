using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Relayer.Pipeline;
using Relayer.Transports;

namespace Relayer.Tests;

public class PublicApiSurfaceTests
{
    [Test]
    public async Task Root_namespace_contains_the_application_surface()
    {
        // Keep the explicit classification in transports/authoring.md in step with this list.
        string[] expected =
        [
            "AsyncApiOptions",
            "Batch",
            "BatchItem",
            "BatchResult",
            "ConsumeBuilderExtensions",
            "ConsumeDelegate",
            "ConsumerBuilder",
            "ConsumerCircuitOptions",
            "DeadLetterMessage",
            "DeadLetterPage",
            "DeadLetterSource",
            "FailureAction",
            "FailureClassifierBuilder",
            "FailureHeaders",
            "FailureOptions",
            "GenerateMessageTypesAttribute",
            "IActivityEnricher",
            "IBatchHandler",
            "IConsumeMiddleware",
            "IConsumeTransactionMiddleware",
            "IConsumerBuilder",
            "IDeadLetterManager",
            "IFailureClassifier",
            "IHandler",
            "IHandlerMarker",
            "IRelayerBuilder",
            "IRequestClient",
            "ISendMiddleware",
            "ISender",
            "ISettlementFeature",
            "ITopology",
            "IUpcaster",
            "JsonSerializerBuilderExtensions",
            "MessageAttribute",
            "MessageContext",
            "MessageDeserializationException",
            "MessageEnvelopeException",
            "MessageEnvelopeMode",
            "MessageFeatures",
            "MessageHeaders",
            "MessageType",
            "OutboxTenantBinding",
            "RelayerServiceCollectionExtensions",
            "RelayerTelemetry",
            "ReplyExtensions",
            "RequestBuilder",
            "RequestOptions",
            "RouteBuilder",
            "SendBuilderExtensions",
            "SendContext",
            "SendDelegate",
            "SendOptions",
            "SendRouteOptions",
            "SendTransactionRequirement",
            "TenantContext",
            "TenantServiceCollectionExtensions",
            "Tombstone",
            "TopologyMode",
            "TopologyReport",
            "TracingBuilderExtensions",
            "TracingOptions",
            "TransportBuilder",
            "TransportSerializerExtensions",
            "UnknownMessageTypeException",
            "WireString",
        ];
        var actual = typeof(IRelayerBuilder).Assembly.GetExportedTypes()
            .Where(type => !type.IsNested && type.Namespace == "Relayer")
            .Select(type => type.Name.Split('`')[0]).Distinct().ToArray();
        await Assert.That(actual).IsEquivalentTo(expected);
    }

    [Test]
    public async Task Adapter_members_are_available_through_the_SPI_only()
    {
        await Assert.That(typeof(IRelayerBuilder).GetMethod("AddTransport")).IsNull();
        await Assert.That(typeof(IRelayerBuilder).GetProperty("MessageTypes")).IsNull();
        await Assert.That(typeof(IRelayerBuilder).GetProperty("Handlers")).IsNull();
        await Assert.That(typeof(IRelayerBuilder).GetProperty("Upcasters")).IsNull();
        await Assert.That(typeof(IConsumerBuilder).GetMethod("AddHandler")).IsNull();
        await Assert.That(typeof(SendOptions).GetProperty("HasNativeConfigurators", BindingFlags.Public | BindingFlags.Instance)).IsNull();
        await Assert.That(typeof(SendOptions).GetMethod("ApplyNative", BindingFlags.Public | BindingFlags.Instance)).IsNull();

        IRelayerInfrastructure? infrastructure = null;
        var services = new ServiceCollection();
        services.AddRelayer(builder => infrastructure = builder.Infrastructure());
        await Assert.That(infrastructure).IsNotNull();
        await Assert.That(infrastructure!.MessageTypes.IsFrozen).IsTrue();
        await Assert.That(typeof(ConsumePipelineBuilder).Namespace).IsEqualTo("Relayer.Pipeline");
        await Assert.That(typeof(OutgoingMessage).Namespace).IsEqualTo("Relayer.Transports");
    }
}
