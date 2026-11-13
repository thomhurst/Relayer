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
    public async Task Rejects_payload_registration_that_would_overwrite_a_tombstone_identity()
    {
        var registry = new MessageTypeRegistry().Add<Tombstone<Ping>>("tests.deleted", 2);

        var exception = await Assert.That(() => registry.Add<Ping>("tests.ping")).Throws<InvalidOperationException>();
        await Assert.That(exception!.Message).Contains("Register only the payload type");

        await AssertWrapperMapping(registry);
        registry.Freeze();
        await AssertWrapperMapping(registry);
    }

    private static async Task AssertWrapperMapping(MessageTypeRegistry registry)
    {
        var identity = new MessageType("tests.deleted", 2);
        await Assert.That(registry.TryGetMessageType(typeof(Tombstone<Ping>), out var actual)).IsTrue();
        await Assert.That(actual).IsEqualTo(identity);
        await Assert.That(registry.TryGetClrType(identity, out var type)).IsTrue();
        await Assert.That(type).IsEqualTo(typeof(Tombstone<Ping>));
        await Assert.That(registry.TryGetMessageType(typeof(Ping), out _)).IsFalse();
        await Assert.That(registry.TryGetClrType(new MessageType("tests.ping", 1), out _)).IsFalse();
    }

    [Test]
    public async Task Rejects_a_separate_tombstone_identity_after_payload_registration()
    {
        var registry = new MessageTypeRegistry().Add<Ping>("tests.ping");

        await Assert.That(() => registry.Add<Tombstone<Ping>>("tests.deleted", 2)).Throws<InvalidOperationException>();

        await AssertPayloadMapping(registry);
        registry.Freeze();
        await AssertPayloadMapping(registry);
    }

    [Test]
    public async Task Repeating_payload_registration_preserves_its_tombstone_alias()
    {
        var registry = new MessageTypeRegistry().Add<Ping>("tests.ping");
        registry.Add<Ping>("tests.ping");

        await AssertPayloadMapping(registry);
        registry.Freeze();
        await AssertPayloadMapping(registry);
    }

    private static async Task AssertPayloadMapping(MessageTypeRegistry registry)
    {
        var identity = new MessageType("tests.ping", 1);
        await Assert.That(registry.TryGetMessageType(typeof(Ping), out var actual)).IsTrue();
        await Assert.That(actual).IsEqualTo(identity);
        await Assert.That(registry.TryGetMessageType(typeof(Tombstone<Ping>), out var alias)).IsTrue();
        await Assert.That(alias).IsEqualTo(identity);
        await Assert.That(registry.TryGetClrType(identity, out var type)).IsTrue();
        await Assert.That(type).IsEqualTo(typeof(Ping));
        await Assert.That(registry.TryGetClrType(new MessageType("tests.deleted", 2), out _)).IsFalse();
        await Assert.That(registry.TryGetMessageType(typeof(Tombstone<Tombstone<Ping>>), out _)).IsFalse();
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
