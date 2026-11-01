using Relayer.Outbox.PostgreSql;

namespace Relayer.Tool;

internal static class RelayerCommand
{
    public static int Run(string[] args, TextWriter output, TextWriter error)
    {
        if (args is ["--help"] or ["-h"])
        {
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
