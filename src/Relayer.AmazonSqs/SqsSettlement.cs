using Relayer.Transports;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Amazon.SQS;

namespace Relayer.AmazonSqs;

internal sealed class SqsSettlement(MessageTracing tracing, MessagingEndpoint endpoint)
{
    private readonly MessageOperationTracing _delete = new(tracing, endpoint, MessagingOperation.Settle, "delete");
    private readonly MessageOperationTracing _visibility = new(tracing, endpoint, MessagingOperation.Settle, "change_visibility");

    internal ValueTask SettleAsync(MessageContext context)
    {
        var native = context.Features.GetRequired<NativeMessage<SqsDelivery>>();
        return SettleAsync(native.Message, native.Disposition, context.CancellationToken);
    }

    internal ValueTask SettleAsync(SqsDelivery delivery, Disposition disposition, CancellationToken token)
    {
        if (disposition.Kind == DispositionKind.Complete && delivery.BatchSlot is { } batch)
        {
            var parent = Activity.Current;
            var activity = _delete.Start();
            if (activity is { IsAllDataRequested: true }) SqsMessageBinding.EnrichActivity(activity, delivery);
            Activity.Current = parent;
            return batch.DeleteAsync(activity);
        }
        return SettleSingleAsync(delivery, disposition, token);
    }

    [RuntimeAsyncMethodGeneration(false)]
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SettleSingleAsync(SqsDelivery delivery, Disposition disposition, CancellationToken token)
    {
        if (disposition.Kind == DispositionKind.SettledByHandler) return;
        if (disposition.Kind is not (DispositionKind.Complete or DispositionKind.Redeliver))
            throw new NotSupportedException("SQS cannot dead-letter a specific message with metadata. Configure Failure.DeadLetterEndpoint to copy durably before deletion.");
        using var activity = (disposition.Kind == DispositionKind.Complete ? _delete : _visibility).Start();
        if (activity is { IsAllDataRequested: true }) SqsMessageBinding.EnrichActivity(activity, delivery);
        try
        {
            if (disposition.Kind == DispositionKind.Complete)
            {
                var response = await delivery.Client.DeleteMessageAsync(delivery.Delete, token).ConfigureAwait(false);
                activity?.SetTag("aws.request_id", response.ResponseMetadata?.RequestId);
            }
            else
            {
                delivery.Visibility.VisibilityTimeout = VisibilitySeconds(delivery, disposition.Delay);
                var response = await delivery.Client.ChangeMessageVisibilityAsync(delivery.Visibility, token).ConfigureAwait(false);
                activity?.SetTag("aws.request_id", response.ResponseMetadata?.RequestId);
            }
        }
        catch (AmazonSQSException error)
        {
            activity?.SetTag("aws.request_id", error.RequestId);
            MessageOperationTracing.Fail(activity, error.ErrorCode ?? "_OTHER");
            throw;
        }
        catch { MessageOperationTracing.Fail(activity, "_OTHER"); throw; }
    }

    internal static int VisibilitySeconds(SqsDelivery delivery, TimeSpan delay)
    {
        if (delay < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(delay));
        var seconds = checked((int)Math.Ceiling(delay.TotalSeconds));
        // SQS measures the ceiling from the original ReceiveMessage request, not a visibility change.
        var remaining = TimeSpan.FromHours(12) - delivery.Time.GetElapsedTime(delivery.ReceivedAt);
        if (seconds > 43_200 || TimeSpan.FromSeconds(seconds) >= remaining)
            throw new InvalidOperationException("SQS visibility redelivery exceeds the remaining twelve-hour receive deadline. Use a shorter delay or durable outbox scheduling.");
        return seconds;
    }
}
