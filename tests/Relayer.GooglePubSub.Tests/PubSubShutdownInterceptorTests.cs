using Google.Cloud.PubSub.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Relayer.GooglePubSub.Tests;

public class PubSubShutdownInterceptorTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Late_ack_or_renewal_is_recorded_without_replacing_the_rpc(bool renewal)
    {
        var observer = new PubSubShutdownInterceptor();
        observer.ReceiverClosed();
        var forwarded = 0;
        if (renewal) await Call(new ModifyAckDeadlineRequest());
        else await Call(new AcknowledgeRequest());
        await Assert.That(forwarded).IsEqualTo(1);
        var failure = await Assert.That(() => observer.AssertClean()).Throws<InvalidOperationException>();
        await Assert.That(failure!.Message).Contains("after native subscriber closure");

        async Task Call<T>(T request) where T : class
        {
            using var call = observer.AsyncUnaryCall(request, Context<T>(), (_, _) =>
            {
                forwarded++;
                return Response(Task.FromResult(new Empty()));
            });
            await call.ResponseAsync;
        }
    }

    [Test]
    public async Task Publisher_disposal_with_pending_send_fails_the_probe()
    {
        var observer = new PubSubShutdownInterceptor();
        var pending = new TaskCompletionSource<Empty>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var call = observer.AsyncUnaryCall(new PublishRequest(), Context<PublishRequest>(), (_, _) => Response(pending.Task));
        observer.PublisherClosed();
        pending.SetResult(new Empty());
        await call.ResponseAsync;
        var failure = await Assert.That(() => observer.AssertClean()).Throws<InvalidOperationException>();
        await Assert.That(failure!.Message).Contains("before admitted RPCs completed");
    }

    [Test]
    public async Task Publisher_closure_requires_the_real_native_shutdown_callback()
    {
        var observer = new PubSubShutdownInterceptor();
        // No RPC is needed to close an unused real native publisher.
        using var channel = Grpc.Net.Client.GrpcChannel.ForAddress("http://localhost:1");
        var api = new PublisherServiceApiClientImpl(new Publisher.PublisherClient(channel), null, null);
        await using var publisher = new PublisherClientImpl(TopicName.FromProjectTopic("probe", "probe"),
            [api], new PublisherClient.Settings(), static () => Task.CompletedTask);
        NativePublisherShutdown.Observe(publisher, observer);
        await Assert.That(() => observer.AssertPublisherClosed()).Throws<InvalidOperationException>();
        await publisher.DisposeAsync();
        observer.AssertPublisherClosed();
    }

    private static ClientInterceptorContext<T, Empty> Context<T>() where T : class => new(
        new Method<T, Empty>(MethodType.Unary, "probe", "probe",
            Marshallers.Create<T>(static _ => [], static _ => throw new NotSupportedException()),
            Marshallers.Create<Empty>(static _ => [], static _ => new())), "unused", default);

    private static AsyncUnaryCall<Empty> Response(Task<Empty> response) => new(response,
        Task.FromResult(new Metadata()), static () => Status.DefaultSuccess, static () => new Metadata(), static () => { });
}
