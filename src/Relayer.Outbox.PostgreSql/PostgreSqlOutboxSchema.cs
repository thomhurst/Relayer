using System.Text;

namespace Relayer.Outbox.PostgreSql;

/// <summary>Creates the shared PostgreSQL outbox, dead outbox, inbox and lease tables.</summary>
public static class PostgreSqlOutboxSchema
{
    /// <summary>Returns idempotent SQL for a new schema. This does not upgrade existing table definitions.</summary>
    /// <remarks>Null schema means public. Identifiers are quoted and must fit PostgreSQL's 63-byte limit.</remarks>
    public static string CreateScript(PostgreSqlOutboxOptions? options = null)
    {
        options ??= new PostgreSqlOutboxOptions();
        var schema = QuoteIdentifier(options.Schema ?? "public");
        ArgumentNullException.ThrowIfNull(options.TablePrefix);
        using var stream = typeof(PostgreSqlOutboxSchema).Assembly.GetManifestResourceStream(
            "Relayer.Outbox.PostgreSql.Schema.sql")!;
        using var reader = new StreamReader(stream);
        var template = reader.ReadToEnd();

        // Replace template tokens in one pass: identifiers may themselves contain token-like text.
        var result = new StringBuilder(template.Length);
        for (var position = 0; position < template.Length;)
        {
            var start = template.IndexOf("{{", position, StringComparison.Ordinal);
            if (start < 0)
            {
                result.Append(template, position, template.Length - position);
                break;
            }
            result.Append(template, position, start - position);
            var end = template.IndexOf("}}", start, StringComparison.Ordinal);
            var token = template[(start + 2)..end];
            result.Append(token == "schema" ? schema : QuoteIdentifier(options.TablePrefix + token));
            position = end + 2;
        }
        return result.ToString();
    }

    internal static string QuoteIdentifier(string identifier)
    {
        if (identifier.Length == 0 || identifier.Contains('\0') || Encoding.UTF8.GetByteCount(identifier) > 63)
            throw new ArgumentException("PostgreSQL identifiers must be nonempty, contain no NUL, and fit in 63 UTF-8 bytes.", nameof(identifier));
        return "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }
}
