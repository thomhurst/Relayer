using Relayer.Transports;
using System.Text;

namespace Relayer.Tests;

public class OutboxSequenceTests
{
    [Test]
    [Arguments("1", 1L)]
    [Arguments("9223372036854775807", long.MaxValue)]
    [Arguments("", null)]
    [Arguments("0", null)]
    [Arguments("-1", null)]
    [Arguments("+1", null)]
    [Arguments(" 1", null)]
    [Arguments("1.0", null)]
    [Arguments("9223372036854775808", null)]
    [Arguments("garbage", null)]
    public async Task Reads_text_and_UTF8_without_requiring_an_envelope(string text, long? expected)
    {
        var headers = new Headers(new WireString(text));
        var context = new Context(headers);
        await Assert.That(headers.Reads).IsEqualTo(0);
        await Assert.That(context.OutboxSequence).IsEqualTo(expected);
        headers.Value = new WireString(Encoding.UTF8.GetBytes(text));
        await Assert.That(context.OutboxSequence).IsEqualTo(expected);
        await Assert.That(headers.Reads).IsEqualTo(2);
    }

    [Test]
    public async Task Missing_header_is_absent()
    {
        await Assert.That(new Context(MessageHeaders.Empty).OutboxSequence).IsNull();
    }

    private sealed class Headers(WireString value) : MessageHeaders
    {
        internal int Reads;
        internal WireString Value = value;
        public override bool TryGetValue(string name, out WireString result)
        {
            Reads++;
            result = Value;
            return name == OutboxHeaders.Sequence;
        }
    }

    private sealed class Context(MessageHeaders headers) : MessageContext
    {
        public override string MessageId => "unused";
        public override MessageType MessageType => default;
        public override MessageHeaders Headers => headers;
        public override int Attempt => 1;
        public override IServiceProvider Services => throw new NotSupportedException();
        public override MessageFeatures Features { get; } = new();
        public override CancellationToken CancellationToken => default;
    }
}
