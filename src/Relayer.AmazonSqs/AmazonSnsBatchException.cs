using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Relayer.Transports;

namespace Relayer.AmazonSqs;

/// <summary>A native batch accepted some entries but rejected others. Successful entries are not resent.</summary>
public sealed class AmazonSnsBatchException : AmazonSimpleNotificationServiceException, IBatchSendFailure
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
    internal int GroupOffset { get; set; }

    /// <inheritdoc />
    public bool IsConfirmed(int messageIndex)
    {
        if (messageIndex < 0) return false;
        // Earlier native chunks completed before this chunk failed.
        if (messageIndex < GroupOffset + BatchOffset) return true;
        if (Response.Successful is not { } successful) return false;
        foreach (var entry in successful)
            if (int.TryParse(entry.Id, System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture, out var index) && index == messageIndex - GroupOffset - BatchOffset)
                return true;
        return false;
    }
}
