using System.Globalization;
using Npgsql;
using Relayer.Outbox.PostgreSql;

namespace Relayer.Tool;

internal static class RelayerCommand
{
    public static async Task<int> RunAsync(string[] args, TextWriter output, TextWriter error)
    {
        if (args.Length < 2 || args[0] != "message" || args[1] != "locate")
            return Run(args, output, error);
        string? id = null, schema = null;
        var prefix = "relayer_";
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 2; i < args.Length; i += 2)
        {
            if (i + 1 == args.Length || !seen.Add(args[i])) return Fail(error, "Repeated or incomplete lookup option.");
            switch (args[i])
            {
                case "--id": id = args[i + 1]; break;
                case "--schema": schema = args[i + 1]; break;
                case "--prefix": prefix = args[i + 1]; break;
                default: return Fail(error, $"Unknown lookup option: {args[i]}");
            }
        }
        if (!Guid.TryParse(id, out var messageId) || messageId == Guid.Empty)
            return Fail(error, "Provide --id with a nonempty message UUID.");
        var connectionString = Environment.GetEnvironmentVariable("RELAYER_POSTGRES_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString)) return Fail(error, "Set RELAYER_POSTGRES_CONNECTION_STRING to the target database connection string.");
        try
        {
            await using var source = NpgsqlDataSource.Create(connectionString);
            var result = await new PostgreSqlMessageLookup(source, new() { Schema = schema, TablePrefix = prefix })
                .FindAsync(messageId).ConfigureAwait(false);
            output.WriteLine($"Message: {result.MessageId}\nState: {result.State}");
            if (result.State == "not present") output.WriteLine("May mean never committed or sent and deleted; inbox receipts are independent evidence.");
            if (result.Destination is not null) output.WriteLine($"Destination: {result.Destination}");
            if (result.DeliverAt is { } due) output.WriteLine($"Due: {due:O}");
            if (result.ClaimToken is { } token) output.WriteLine($"Claim: {token}; expires: {result.ClaimExpiresAt:O}");
            output.WriteLine($"Attempts: {result.Attempts}; last error: {result.LastError}");
            if (result.FailedAt is { } failed) output.WriteLine($"Failed: {failed:O}");
            if (result.Lane is { } lane) output.WriteLine($"Lane: {lane.ToString(CultureInfo.InvariantCulture)}; owner: {result.LaneOwner}; expires: {result.LaneExpiresAt:O}; earlier rows: {result.EarlierRows?.ToString(CultureInfo.InvariantCulture) ?? "unknown"}");
            foreach (var receipt in result.InboxReceipts) output.WriteLine($"Inbox: {receipt.Consumer}; processed: {receipt.ProcessedAt:O}");
            return 0;
        }
        catch (ArgumentException exception) { return Fail(error, exception.Message); }
        catch (NpgsqlException) { return Fail(error, "PostgreSQL lookup failed. Check database access and apply the Relayer schema first."); }
    }

    public static int Run(string[] args, TextWriter output, TextWriter error)
    {
        if (args is ["--help"] or ["-h"])
        {
            output.WriteLine("dotnet relayer message locate --id <UUID> [--schema public] [--prefix relayer_] (uses RELAYER_POSTGRES_CONNECTION_STRING)");
            output.WriteLine("dotnet relayer db script --provider postgres [--schema public] [--prefix relayer_]");
            return 0;
        }
        if (args.Length < 2 || args[0] != "db" || args[1] != "script")
            return Fail(error, "Expected: db script --provider postgres [--schema public] [--prefix relayer_]");

        string? provider = null;
        string? schema = null;
        var prefix = "relayer_";
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 2; i < args.Length; i += 2)
        {
            if (i + 1 == args.Length || args[i] is not ("--provider" or "--schema" or "--prefix") || !seen.Add(args[i]))
                return Fail(error, $"Unknown, repeated or incomplete option: {args[i]}");
            switch (args[i])
            {
                case "--provider": provider = args[i + 1]; break;
                case "--schema": schema = args[i + 1]; break;
                case "--prefix": prefix = args[i + 1]; break;
            }
        }
        if (provider != "postgres")
            return Fail(error, "Only --provider postgres is supported. SQL Server and MySQL providers are planned.");
        try
        {
            output.Write(PostgreSqlOutboxSchema.CreateScript(new PostgreSqlOutboxOptions { Schema = schema, TablePrefix = prefix }));
            return 0;
        }
        catch (ArgumentException exception)
        {
            return Fail(error, exception.Message);
        }
    }

    private static int Fail(TextWriter error, string message)
    {
        error.WriteLine(message);
        return 1;
    }
}
