namespace Relayer.Tests;

public class SendTransportTests
{
    [Test]
    public async Task Applies_native_callbacks_in_registration_order_without_a_wrapper()
    {
        var options = new SendOptions()
            .ConfigureNative<NativeMessage>(static native => native.Value = 2)
            .ConfigureNative<NativeMessage>(static native => native.Value *= 3);
        var native = new NativeMessage();

        options.ApplyNative(native);

        await Assert.That(options.HasNativeConfigurators).IsTrue();
        await Assert.That(native.Value).IsEqualTo(6);
    }

    [Test]
    public async Task Native_callback_for_a_base_type_applies_to_derived_native_messages()
    {
        var options = new SendOptions().ConfigureNative<NativeMessage>(static native => native.Value = 7);
        var native = new DerivedNativeMessage();

        options.ApplyNative(native);

        await Assert.That(native.Value).IsEqualTo(7);
    }

    [Test]
    public async Task Mismatched_native_callback_fails_instead_of_being_ignored()
    {
        var options = new SendOptions().ConfigureNative<NativeMessage>(static _ => { });

        await Assert.That(() => options.ApplyNative(new object()))
            .Throws<InvalidOperationException>()
            .WithMessageContaining("native outgoing type");
    }

    [Test]
    public async Task Null_native_message_and_callback_are_rejected()
    {
        var options = new SendOptions();

        await Assert.That(() => options.ConfigureNative<NativeMessage>(null!)).Throws<ArgumentNullException>();
        await Assert.That(() => options.ApplyNative<NativeMessage>(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Outgoing_message_borrows_body_and_headers_and_preserves_delivery_fields()
    {
        byte[] body = [1, 2, 3];
        KeyValuePair<string, WireString>[] headers =
        [
            new("ce_id", new WireString("019a3c1b-97c0-7000-8000-000000000001")),
            new("ce_type", new WireString("tests.order;v=2")),
            new("traceparent", new WireString("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"))
        ];
        var deliverAt = DateTimeOffset.UtcNow.AddMinutes(5);
        var outgoing = new OutgoingMessage("orders", body, headers, "customer-1", deliverAt);

        await Assert.That(outgoing.Destination).IsEqualTo("orders");
        await Assert.That(outgoing.Body.Equals(body.AsMemory())).IsTrue();
        await Assert.That(outgoing.Headers.Equals(headers.AsMemory())).IsTrue();
        await Assert.That(outgoing.OrderingKey).IsEqualTo("customer-1");
        await Assert.That(outgoing.DeliverAt).IsEqualTo(deliverAt);
        await Assert.That(outgoing.Headers.Span[0].Value.Text).IsEqualTo(headers[0].Value.Text);
        outgoing.ValidateForOutbox();
    }

    [Test]
    public async Task Outgoing_message_applies_native_callbacks_but_rejects_outbox_enlistment()
    {
        var options = new SendOptions().ConfigureNative<NativeMessage>(static native => native.Value = 42);
        var outgoing = new OutgoingMessage("orders", ReadOnlyMemory<byte>.Empty, options: options);
        var native = new NativeMessage();

        outgoing.ApplyNative(native);

        await Assert.That(native.Value).IsEqualTo(42);
        await Assert.That(() => outgoing.ValidateForOutbox())
            .Throws<InvalidOperationException>()
            .WithMessageContaining("cannot be persisted");
    }

    [Test]
    public async Task Portable_options_can_be_enlisted_and_empty_native_configuration_is_a_no_op()
    {
        var options = new SendOptions { OrderingKey = "customer-1", DeliverAt = DateTimeOffset.UtcNow };
        options.Headers.Add("tenant", "tenant-1");
        var outgoing = new OutgoingMessage("orders", ReadOnlyMemory<byte>.Empty, options: options);

        outgoing.ValidateForOutbox();
        outgoing.ApplyNative(new NativeMessage());

        await Assert.That(options.HasNativeConfigurators).IsFalse();
        await Assert.That(outgoing.Headers.IsEmpty).IsTrue();
    }

    [Test]
    public async Task Empty_destination_is_rejected()
    {
        await Assert.That(() => new OutgoingMessage(" ", ReadOnlyMemory<byte>.Empty))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task Applying_preconfigured_native_callbacks_allocates_zero_bytes()
    {
        var options = new SendOptions().ConfigureNative<NativeMessage>(static native => native.Value = 42);
        var outgoing = new OutgoingMessage("orders", ReadOnlyMemory<byte>.Empty, options: options);
        var native = new NativeMessage();
        for (var i = 0; i < 1000; i++)
        {
            outgoing.ApplyNative(native);
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            outgoing.ApplyNative(native);
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        await Assert.That(allocated).IsEqualTo(0L);
        await Assert.That(native.Value).IsEqualTo(42);
    }

    [Test]
    public async Task Outgoing_message_without_options_applies_no_native_callback()
    {
        var outgoing = new OutgoingMessage("orders", ReadOnlyMemory<byte>.Empty);
        var native = new NativeMessage { Value = 7 };

        outgoing.ApplyNative(native);
        outgoing.ValidateForOutbox();

        await Assert.That(native.Value).IsEqualTo(7);
        await Assert.That(() => outgoing.ApplyNative<NativeMessage>(null!)).Throws<ArgumentNullException>();
    }

    private class NativeMessage
    {
        public int Value { get; set; }
    }

    private sealed class DerivedNativeMessage : NativeMessage;
}
