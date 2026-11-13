using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace Relayer.GooglePubSub.Tests;

public class PubSubStreamReadinessTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Initial_write_readiness_waits_for_native_completion_and_preserves_failure(bool fail)
    {
        var interceptor = new PubSubShutdownInterceptor();
        var writer = new PendingWriter();
        using var call = interceptor.AsyncDuplexStreamingCall<StreamingPullRequest, StreamingPullResponse>(
            new ClientInterceptorContext<StreamingPullRequest, StreamingPullResponse>(null!, null!, default),
            _ => new(writer, null!, Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => new(), () => { }));
        var writing = call.RequestStream.WriteAsync(new() { Subscription = "projects/test/subscriptions/test" });
        await Assert.That(interceptor.InitialWriteCompleted.Task.IsCompleted).IsFalse();
        var failure = new IOException("native write failed");
        if (fail) writer.Completion.SetException(failure);
        else writer.Completion.SetResult();
        if (fail) await Assert.That(async () => await writing).Throws<IOException>();
        else await writing;
        await interceptor.InitialWriteCompleted.Task;
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Restart_waits_for_its_own_write_even_when_previous_stream_completes(bool completePreviousBeforeRestart)
    {
        var interceptor = new PubSubShutdownInterceptor();
        interceptor.ReceiverStarting();
        var previous = new PendingWriter();
        using var previousCall = interceptor.AsyncDuplexStreamingCall<StreamingPullRequest, StreamingPullResponse>(
            new ClientInterceptorContext<StreamingPullRequest, StreamingPullResponse>(null!, null!, default),
            _ => new(previous, null!, Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => new(), () => { }));
        var previousWrite = previousCall.RequestStream.WriteAsync(new() { Subscription = "projects/test/subscriptions/test" });
        if (completePreviousBeforeRestart)
        {
            previous.Completion.SetResult();
            await previousWrite;
        }
        interceptor.ReceiverStarting();
        var current = new PendingWriter();
        using var currentCall = interceptor.AsyncDuplexStreamingCall<StreamingPullRequest, StreamingPullResponse>(
            new ClientInterceptorContext<StreamingPullRequest, StreamingPullResponse>(null!, null!, default),
            _ => new(current, null!, Task.FromResult(new Metadata()), () => Status.DefaultSuccess, () => new(), () => { }));
        var currentWrite = currentCall.RequestStream.WriteAsync(new() { Subscription = "projects/test/subscriptions/test" });
        if (!completePreviousBeforeRestart)
        {
            previous.Completion.SetResult();
            await previousWrite;
        }
        await Assert.That(interceptor.InitialWriteCompleted.Task.IsCompleted).IsFalse();
        current.Completion.SetResult();
        await currentWrite;
        await Assert.That(interceptor.InitialWriteCompleted.Task.IsCompleted).IsTrue();
    }

    private sealed class PendingWriter : IClientStreamWriter<StreamingPullRequest>
    {
        internal TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public WriteOptions? WriteOptions { get; set; }
        public Task WriteAsync(StreamingPullRequest message) => Completion.Task;
        public Task CompleteAsync() => Task.CompletedTask;
    }
}
