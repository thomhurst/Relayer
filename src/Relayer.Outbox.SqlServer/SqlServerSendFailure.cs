using Microsoft.Extensions.Logging;

namespace Relayer.Outbox.SqlServer;

internal static partial class SqlServerSendFailure
{
    internal static bool IsRetryable(Func<Exception, bool>? classify, Exception error, ILogger logger)
    {
        try { return classify?.Invoke(error) == true; }
        catch (Exception classificationError)
        {
            ClassificationFailed(logger, classificationError);
            return false;
        }
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Outbox send failure classification threw; recording the original send failure against the poison budget.")]
    private static partial void ClassificationFailed(ILogger logger, Exception exception);
}
