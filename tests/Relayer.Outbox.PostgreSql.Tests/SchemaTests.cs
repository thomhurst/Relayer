using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Relayer.Outbox.EntityFrameworkCore;
using Relayer.Outbox.PostgreSql;
using Relayer.Tool;

namespace Relayer.Outbox.PostgreSql.Tests;

public class SchemaTests
{
    [Test]
    public async Task EF_migration_rejects_other_database_providers()
    {
        var migration = new MigrationBuilder("Microsoft.EntityFrameworkCore.SqlServer");
        await Assert.That(() => migration.CreateRelayerPostgreSqlSchema()).Throws<InvalidOperationException>();
        await Assert.That(migration.Operations.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Cli_and_EF_migration_use_the_same_embedded_script()
    {
        var options = new PostgreSqlOutboxOptions { Schema = "a\";--{{schema}}", TablePrefix = "{{outbox}}_" };
        var script = PostgreSqlOutboxSchema.CreateScript(options);
        using var output = new StringWriter();
        using var error = new StringWriter();
        var exit = RelayerCommand.Run(["db", "script", "--provider", "postgres", "--schema", options.Schema,
            "--prefix", options.TablePrefix], output, error);
        await Assert.That(exit).IsEqualTo(0);
        await Assert.That(output.ToString()).IsEqualTo(script);
        await Assert.That(error.ToString()).IsEqualTo("");
        await Assert.That(script).Contains("\"a\"\";--{{schema}}\".\"{{outbox}}_outbox\"");

        var migration = new MigrationBuilder("Npgsql.EntityFrameworkCore.PostgreSQL");
        migration.CreateRelayerPostgreSqlSchema(options);
        await Assert.That(migration.Operations.Count).IsEqualTo(1);
        await Assert.That(((SqlOperation)migration.Operations[0]).Sql).IsEqualTo(script);
        await Assert.That(((SqlOperation)migration.Operations[0]).SuppressTransaction).IsFalse();
    }

    [Test]
    [Arguments("")]
    [Arguments("bad\0schema")]
    [Arguments("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl")]
    [Arguments("éééééééééééééééééééééééééééééééé")]
    public async Task Invalid_schema_identifiers_fail_before_emitting_SQL(string schema)
    {
        await Assert.That(() => PostgreSqlOutboxSchema.CreateScript(new PostgreSqlOutboxOptions { Schema = schema }))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task Generated_index_names_cannot_be_silently_truncated()
    {
        await Assert.That(() => PostgreSqlOutboxSchema.CreateScript(new PostgreSqlOutboxOptions { TablePrefix = new string('x', 44) }))
            .Throws<ArgumentException>();
        await Assert.That(() => PostgreSqlOutboxSchema.CreateScript(new PostgreSqlOutboxOptions { TablePrefix = null! }))
            .Throws<ArgumentNullException>();
    }

    [Test]
    [Arguments("sqlserver")]
    [Arguments("mysql")]
    public async Task Unsupported_providers_emit_no_SQL(string provider)
    {
        using var output = new StringWriter();
        using var error = new StringWriter();
        await Assert.That(RelayerCommand.Run(["db", "script", "--provider", provider], output, error)).IsEqualTo(1);
        await Assert.That(output.ToString()).IsEqualTo("");
        await Assert.That(error.ToString()).Contains("Only --provider postgres");
    }

    [Test]
    public async Task Invalid_cli_arguments_emit_no_SQL()
    {
        string[][] cases = [[], ["db"], ["db", "script"], ["db", "script", "--provider"],
            ["db", "script", "--unknown", "x"], ["db", "script", "--provider", "postgres", "--provider", "postgres"],
            ["db", "script", "--provider", "postgres", "--schema", ""]];
        foreach (var args in cases)
        {
            using var output = new StringWriter();
            using var error = new StringWriter();
            await Assert.That(RelayerCommand.Run(args, output, error)).IsEqualTo(1);
            await Assert.That(output.ToString()).IsEqualTo("");
        }
    }
}
