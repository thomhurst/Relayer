namespace Relayer.Tests;

public class MessageTypeRegistryTests
{
    [Test]
    public async Task Maps_types_to_identities_in_both_directions()
    {
        var registry = new MessageTypeRegistry().Add<Ping>("tests.ping", 2);
        registry.Freeze();

        await Assert.That(registry.TryGetMessageType(typeof(Ping), out var identity)).IsTrue();
        await Assert.That(identity).IsEqualTo(new MessageType("tests.ping", 2));
        await Assert.That(registry.TryGetMessageType(typeof(Tombstone<Ping>), out var tombstone)).IsTrue();
        await Assert.That(tombstone).IsEqualTo(identity);
        await Assert.That(registry.TryGetClrType(identity, out var type)).IsTrue();
        await Assert.That(type).IsEqualTo(typeof(Ping));
    }

    [Test]
    public async Task Rejects_the_same_type_under_a_different_identity()
    {
        var registry = new MessageTypeRegistry().Add<Ping>("tests.ping");

        await Assert.That(() => registry.Add<Ping>("tests.other")).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Rejects_the_same_identity_for_a_different_type()
    {
        var registry = new MessageTypeRegistry().Add<Ping>("tests.ping");

        await Assert.That(() => registry.Add<Pong>("tests.ping")).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Rejects_changes_after_freezing()
    {
        var registry = new MessageTypeRegistry();
        registry.Freeze();

        await Assert.That(() => registry.Add<Ping>("tests.ping")).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Formats_identity_as_ce_type()
    {
        await Assert.That(new MessageType("billing.charge-card", 2).ToString()).IsEqualTo("billing.charge-card;v=2");
    }

    private sealed record Ping;
    private sealed record Pong;
}
