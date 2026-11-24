using System.Buffers;
using System.Text;
using Relayer.Testing;
using Relayer.Transports;
using TUnit.Assertions.Enums;

namespace Relayer.Conformance.Tests;

public class SettlementConformanceTests
{
    [Test]
    public async Task Settlement_rejects_original_replay_during_restart_drain()
    {
        var fixture = new SettlementFixture(replayDuringDrain: true);
        var suite = new TransportConformanceSuite<OutgoingMessage, TestBinding>((_, _) => new(fixture));

        var error = await Assert.That(async () =>
        {
            await suite.RunAsync(TransportConformanceCase.SendReceiveSettle);
        }).Throws<InvalidOperationException>();

        await Assert.That(error!.InnerException!.Message).Contains("Original message replayed after the restart marker");
        await Assert.That(fixture.SettledBodies)
            .IsEquivalentTo(["{\"Value\":1}", "{\"Value\":2}", "{\"Value\":1}"], CollectionOrdering.Matching);
    }

    [Test]
    public async Task Settlement_accepts_healthy_restart_and_checks_native_completion()
    {
        var fixture = new SettlementFixture(replayDuringDrain: false);
        var suite = new TransportConformanceSuite<OutgoingMessage, TestBinding>((_, _) => new(fixture));

        var result = await suite.RunAsync(TransportConformanceCase.SendReceiveSettle);

        await Assert.That(result.Skipped).IsFalse();
        await Assert.That(fixture.SettlementVerified).IsTrue();
        await Assert.That(fixture.SettledBodies).IsEquivalentTo(["{\"Value\":1}", "{\"Value\":2}"], CollectionOrdering.Matching);
    }

    private readonly struct TestBinding : IMessageBinding<OutgoingMessage>
    {
        public static ReadOnlySequence<byte> GetBody(OutgoingMessage message)
        {
            return new(message.Body);
        }

        public static int? GetDeliveryCount(OutgoingMessage message)
        {
            return null;
        }

        public static bool TryGetHeader(OutgoingMessage message, string name, out WireString value)
        {
            foreach (var header in message.Headers.Span)
            {
                if (header.Key == name)
                {
                    value = header.Value;
                    return true;
                }
            }

            value = default;
            return false;
        }
    }

    private sealed class SettlementFixture(bool replayDuringDrain) : ITransportConformanceFixture<OutgoingMessage>, ITransport
    {
        private Func<OutgoingMessage, CancellationToken, ValueTask<Disposition>> _receive = null!;
        private OutgoingMessage? _replay;
        private int _starts;
        private int _pending;

        internal List<string> SettledBodies
        {
            get;
        } = [];

        internal bool SettlementVerified
        {
            get; private set;
        }

        public ITransport Transport => this;

        public string Name => "conformance";

        public string Destination => "settlement";

        public string MessagingSystem => "test";

        public string SettlementOperation => "complete";

        public HeaderNames HeaderNames
        {
            get;
        } = new();

        public void RegisterConsumer(Func<OutgoingMessage, CancellationToken, ValueTask<Disposition>> receive)
        {
            _receive = receive;
        }

        public async ValueTask SendAsync(OutgoingMessage message, CancellationToken token)
        {
            if (replayDuringDrain && _starts == 1)
            {
                _replay = new(message.Destination, message.Body.ToArray(), message.Headers.ToArray());
            }

            await DeliverAsync(message, token);
        }

        public ValueTask SettleAsync(OutgoingMessage incoming, Disposition disposition, CancellationToken token)
        {
            if (disposition.Kind != DispositionKind.Complete)
            {
                throw new InvalidOperationException("Expected the delivery to complete.");
            }

            SettledBodies.Add(Encoding.UTF8.GetString(incoming.Body.Span));
            _pending--;
            return ValueTask.CompletedTask;
        }

        public Task StartAsync(CancellationToken token)
        {
            _starts++;
            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken token)
        {
            if (_starts == 2 && _replay is { } replay)
            {
                // The marker was settled in SendAsync. Only this drain delivers the original again.
                _replay = null;
                await DeliverAsync(replay, token);
            }
        }

        public ValueTask VerifySettlementAsync(CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            if (_pending != 0)
            {
                throw new InvalidOperationException("The queue still has unsettled messages.");
            }

            SettlementVerified = true;
            return ValueTask.CompletedTask;
        }

        private async ValueTask DeliverAsync(OutgoingMessage message, CancellationToken token)
        {
            _pending++;
            await _receive(message, token);
        }

        public TransportCapabilities GetCapabilities(string endpoint)
        {
            return new();
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask<TopologyReport> ApplyTopologyAsync(TopologyMode mode, CancellationToken token)
        {
            throw new NotSupportedException();
        }

        public ValueTask SendBatchAsync(ReadOnlyMemory<OutgoingMessage> messages, CancellationToken token)
        {
            throw new NotSupportedException();
        }

        public ValueTask<ConformanceDeadLetter> ReadDeadLetterAsync(CancellationToken token)
        {
            throw new NotSupportedException();
        }

        public Task DisconnectAsync(CancellationToken token)
        {
            throw new NotSupportedException();
        }

        public Task ReconnectAsync(CancellationToken token)
        {
            throw new NotSupportedException();
        }
    }
}
