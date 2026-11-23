using Microsoft.Extensions.DependencyInjection;
using Relayer.Pipeline;
using Relayer.Transports;

namespace Relayer.Tests;

[Message("tests.order-placed", Version = 2)]
public sealed record OrderPlaced(Guid OrderId);

[Message("tests.order-cancelled")]
public sealed record OrderCancelled(Guid OrderId);

public class ConfigurationTests
{
    [Test]
    public async Task Source_generator_registers_message_types_from_this_assembly()
    {
        var services = new ServiceCollection();
        services.AddRelayer(relayer => relayer.AddMessageTypesFromThisAssembly());

        await using var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<MessageTypeRegistry>();

        await Assert.That(registry.IsFrozen).IsTrue();
        await Assert.That(registry.TryGetMessageType(typeof(OrderPlaced), out var placed)).IsTrue();
        await Assert.That(placed).IsEqualTo(new MessageType("tests.order-placed", 2));
        await Assert.That(registry.TryGetMessageType(typeof(OrderCancelled), out var cancelled)).IsTrue();
        await Assert.That(cancelled).IsEqualTo(new MessageType("tests.order-cancelled", 1));
    }

    [Test]
    public async Task Defaults_topology_mode_to_validate()
    {
        TopologyMode? mode = null;
        new ServiceCollection().AddRelayer(relayer => mode = relayer.TopologyMode);

        await Assert.That(mode).IsEqualTo(TopologyMode.Validate);
    }

    [Test]
    public async Task AddRelayer_twice_throws()
    {
        var services = new ServiceCollection().AddRelayer(_ =>
        {
        });

        await Assert.That(() => services.AddRelayer(_ =>
        {
        })).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Nested_AddRelayer_on_the_same_collection_throws_and_allows_retry()
    {
        var services = new ServiceCollection();
        await Assert.That(() => services.AddRelayer(outer => outer.Services.AddRelayer(_ =>
        {
        }))).Throws<InvalidOperationException>().WithMessageContaining("AddRelayer has already been called");

        services.AddRelayer(_ =>
        {
        });

        await using var provider = services.BuildServiceProvider();
        await Assert.That(provider.GetServices<RelayerConfiguration>().Count()).IsEqualTo(1);
    }

    [Test]
    public async Task Native_configuration_for_the_wrong_transport_type_throws()
    {
        var options = new SendOptions().ConfigureNative<StringBuilderStandIn>(_ =>
        {
        });

        await Assert.That(() => options.ApplyNative(new object()))
            .Throws<InvalidOperationException>()
            .WithMessageContaining("native outgoing type");
    }

    [Test]
    public async Task Native_configuration_is_applied_to_matching_type()
    {
        var native = new StringBuilderStandIn();
        new SendOptions().ConfigureNative<StringBuilderStandIn>(n => n.Value = "set").ApplyNative(native);

        await Assert.That(native.Value).IsEqualTo("set");
    }

    private sealed class StringBuilderStandIn
    {
        public string? Value
        {
            get; set;
        }
    }
}
