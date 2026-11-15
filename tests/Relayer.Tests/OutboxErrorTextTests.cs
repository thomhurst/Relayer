using Relayer.Transports;

namespace Relayer.Tests;

public class OutboxErrorTextTests
{
    [Test]
    public async Task Records_the_exception_type_with_its_message()
    {
        await Assert.That(OutboxErrorText.Describe(new TimeoutException("broker offline")))
            .IsEqualTo("System.TimeoutException: broker offline");
    }

    [Test]
    public async Task Appends_the_innermost_exception()
    {
        var error = new InvalidOperationException("send failed",
            new IOException("wrapped", new TimeoutException("socket timed out")));
        await Assert.That(OutboxErrorText.Describe(error))
            .IsEqualTo("System.InvalidOperationException: send failed ---> System.TimeoutException: socket timed out");
    }

    [Test]
    public async Task Bounds_the_text_without_splitting_a_surrogate_pair()
    {
        var prefix = "System.Exception: ";
        var message = new string('x', OutboxErrorText.MaxChars - prefix.Length - 1) + "\U0001F600tail";
        var text = OutboxErrorText.Describe(new Exception(message));
        await Assert.That(text.Length).IsEqualTo(OutboxErrorText.MaxChars - 1);
        await Assert.That(char.IsHighSurrogate(text[^1])).IsFalse();
    }
}
