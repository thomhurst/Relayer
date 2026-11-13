using System.Net.Http;
using Grpc.Core;

namespace Relayer.GooglePubSub.Tests;

public class PubSubOutageFailureTests
{
    [Test]
    public async Task Bare_native_handshake_failure_requires_an_active_fixture_outage()
    {
        var cause = new HttpRequestException("An HTTP/2 connection could not be established because the server did not complete the HTTP/2 handshake.");
        var error = new RpcException(new Status(StatusCode.Internal, "Error starting gRPC call.", cause));
        await Assert.That(PubSubFixture.IsExpectedPublishFailure(error, disconnected: true)).IsTrue();
        await Assert.That(PubSubFixture.IsExpectedPublishFailure(error)).IsFalse();
        var unrelated = new RpcException(new Status(StatusCode.Internal, "Other error", new HttpRequestException("Invalid request")));
        await Assert.That(PubSubFixture.IsExpectedPublishFailure(unrelated, disconnected: true)).IsFalse();
        var detailOnly = new RpcException(new Status(StatusCode.Internal, cause.Message));
        await Assert.That(PubSubFixture.IsExpectedPublishFailure(detailOnly, disconnected: true)).IsFalse();
    }

    [Test]
    [Arguments(HttpRequestError.InvalidResponse)]
    [Arguments(HttpRequestError.ResponseEnded)]
    public async Task Internal_publish_with_typed_connection_failure_is_an_expected_outage(HttpRequestError kind)
    {
        var cause = new HttpRequestException("Error starting gRPC call.", new HttpIOException(kind, "Connection interrupted."));
        var error = new RpcException(new Status(StatusCode.Internal, "Error starting gRPC call.", cause));
        await Assert.That(PubSubFixture.IsExpectedPublishFailure(error)).IsTrue();
    }

    [Test]
    public async Task Unrelated_internal_errors_are_not_expected_outages()
    {
        await Assert.That(PubSubFixture.IsExpectedPublishFailure(new RpcException(new Status(StatusCode.Internal,
            "An HTTP/2 connection could not be established because the server did not complete the HTTP/2 handshake.")))).IsFalse();
        var unrelatedIo = new RpcException(new Status(StatusCode.Internal, "Unrelated error", new IOException("Unrelated IO error")));
        var invalidRequest = new RpcException(new Status(StatusCode.Internal, "Invalid request", new HttpRequestException("Invalid request")));
        await Assert.That(PubSubFixture.IsExpectedPublishFailure(unrelatedIo)).IsFalse();
        await Assert.That(PubSubFixture.IsExpectedPublishFailure(invalidRequest)).IsFalse();
    }
}
