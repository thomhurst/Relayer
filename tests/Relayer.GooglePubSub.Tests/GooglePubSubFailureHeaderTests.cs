using System.Text;
using Google.Cloud.PubSub.V1;
using Encoding = System.Text.Encoding;

namespace Relayer.GooglePubSub.Tests;

public class GooglePubSubFailureHeaderTests
{
    [Test]
    public async Task Large_unicode_history_round_trips_losslessly_and_shorter_replacement_removes_old_parts()
    {
        var message = new PubsubMessage();
        var history = string.Concat(Enumerable.Repeat("exception 😀 日本語\n", 500));
        GooglePubSubFailureHeaders.Write(message, history);
        GooglePubSubFailureHeaders.Validate(message);
        await Assert.That(message.Attributes.ContainsKey(FailureHeaders.History)).IsFalse();
        await Assert.That(message.Attributes.Values.All(value => Encoding.UTF8.GetByteCount(value) <= 1024)).IsTrue();
        await Assert.That(GooglePubSubBinding.TryGetHeader(new(message, null!), FailureHeaders.History, out var read)).IsTrue();
        await Assert.That(read.ToString()).IsEqualTo(history);
        GooglePubSubFailureHeaders.Write(message, "short");
        await Assert.That(message.Attributes.Count).IsEqualTo(1);
        await Assert.That(message.Attributes[FailureHeaders.History]).IsEqualTo("short");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Failure_copy_removes_all_old_history_before_budgeting_even_without_replacement(bool split)
    {
        var message = new PubsubMessage();
        message.Attributes["application"] = "preserved";
        GooglePubSubFailureHeaders.Write(message, new string('x', split ? 4096 : 32));
        message.Attributes[FailureHeaders.History + ".part.999"] = "orphan";
        var copy = GooglePubSubHostedService.Copy(message);
        await Assert.That(copy.Headers.Length).IsEqualTo(1);
        await Assert.That(copy.Headers.Span[0].Key).IsEqualTo("application");
        await Assert.That(copy.Headers.Span[0].Value.ToString()).IsEqualTo("preserved");
    }

    [Test]
    public async Task Malformed_or_missing_parts_are_treated_as_missing_history()
    {
        var message = new PubsubMessage();
        message.Attributes[GooglePubSubFailureHeaders.Parts] = "101";
        await Assert.That(GooglePubSubFailureHeaders.TryRead(message, out _)).IsFalse();
        message.Attributes[GooglePubSubFailureHeaders.Parts] = "2";
        await Assert.That(GooglePubSubFailureHeaders.TryRead(message, out _)).IsFalse();
        message.Attributes[FailureHeaders.History + ".part.999"] = "orphan";
        GooglePubSubFailureHeaders.Write(message, "replacement");
        await Assert.That(message.Attributes.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Insufficient_native_attribute_capacity_fails_without_truncating_history()
    {
        var message = new PubsubMessage();
        for (var i = 0; i < 99; i++) message.Attributes["application-" + i] = "value";
        GooglePubSubFailureHeaders.Write(message, new string('x', 2048));
        await Assert.That(() => GooglePubSubFailureHeaders.Validate(message)).Throws<InvalidOperationException>().WithMessageContaining("100 native attributes");
    }
}
