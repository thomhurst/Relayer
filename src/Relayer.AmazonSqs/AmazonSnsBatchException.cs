using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;

namespace Relayer.AmazonSqs;

/// <summary>A native batch accepted some entries but rejected others. Successful entries are not resent.</summary>
public sealed class AmazonSnsBatchException : AmazonSimpleNotificationServiceException
{
    internal AmazonSnsBatchException(PublishBatchResponse response, int batchOffset)
        : base($"SNS PublishBatch rejected {response.Failed.Count} entries. Inspect Response.Failed; earlier entries may already be delivered.")
    {
        Response = response;
        BatchOffset = batchOffset;
        RequestId = response.ResponseMetadata?.RequestId;
    }

    /// <summary>The unmodified native response, including successful and failed entry IDs.</summary>
    public PublishBatchResponse Response { get; }
    /// <summary>The starting index of this native batch within its contiguous destination group.</summary>
    public int BatchOffset { get; }
}
