using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Relayer.SourceGenerators.Tests;

public class PreviewApiTests
{
    [Test]
    [Arguments("Relayer.Sagas.SagaData", "RLYEXP001")]
    [Arguments("Relayer.Sagas.EntityFrameworkCore.EntityFrameworkCoreSagaData", "RLYEXP001")]
    [Arguments("Relayer.Outbox.PostgreSql.PostgreSqlLocalOptions", "RLYEXP002")]
    [Arguments("Relayer.Outbox.PostgreSql.PostgreSqlLocalConsumerOptions", "RLYEXP002")]
    [Arguments("Relayer.Outbox.PostgreSql.PostgreSqlLocalDelivery", "RLYEXP002")]
    [Arguments("Relayer.Outbox.PostgreSql.PostgreSqlLocalMessageBinding", "RLYEXP002")]
    [Arguments("Relayer.Outbox.PostgreSql.PostgreSqlChangeCaptureOptions", "RLYEXP003")]
    [Arguments("Relayer.Outbox.PostgreSql.ITenantSource", "RLYEXP004")]
    [Arguments("Relayer.Outbox.PostgreSql.PostgreSqlTenantStore", "RLYEXP004")]
    [Arguments("Relayer.Outbox.PostgreSql.PostgreSqlTenantDispatchOptions", "RLYEXP004")]
    public async Task Consumer_must_explicitly_opt_in(string type, string id)
    {
        var compilation = Compile($"class Consumer {{ System.Type Feature => typeof({type}); }}");
        var diagnostics = compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error).ToArray();
        await Assert.That(diagnostics.Length).IsEqualTo(1);
        await Assert.That(diagnostics[0].Id).IsEqualTo(id);
        await Assert.That(diagnostics[0].Descriptor.HelpLinkUri)
            .IsEqualTo($"https://relayer.tomlonghurst.dev/docs/reference/diagnostics#{id.ToLowerInvariant()}");
        var optedIn = compilation.WithOptions(compilation.Options.WithSpecificDiagnosticOptions(
            new Dictionary<string, ReportDiagnostic> { [id] = ReportDiagnostic.Suppress }));
        await Assert.That(optedIn.GetDiagnostics().Any(d => d.Severity == DiagnosticSeverity.Error)).IsFalse();
    }

    [Test]
    public async Task Stable_PostgreSql_registration_remains_available_without_opt_in()
    {
        var compilation = Compile("""
            class Consumer
            {
                System.Type Options => typeof(Relayer.Outbox.PostgreSql.PostgreSqlOutboxOptions);
                void Register(Microsoft.Extensions.DependencyInjection.IServiceCollection services) =>
                    Relayer.Outbox.PostgreSql.PostgreSqlOutboxRegistration.AddRelayerPostgreSqlOutbox(services);
            }
            """);
        await Assert.That(compilation.GetDiagnostics().Any(d => d.Severity == DiagnosticSeverity.Error)).IsFalse();
    }

    [Test]
    [Arguments("AddRelayerPostgreSqlChangeCapture", "RLYEXP003")]
    [Arguments("AddRelayerPostgreSqlTenantDispatch", "RLYEXP004")]
    public async Task Every_preview_registration_overload_is_marked(string method, string id)
    {
        var compilation = Compile("");
        var type = compilation.GetTypeByMetadataName("Relayer.Outbox.PostgreSql.PostgreSqlOutboxServiceCollectionExtensions")!;
        var overloads = type.GetMembers(method).OfType<IMethodSymbol>().ToArray();
        await Assert.That(overloads.Length).IsEqualTo(2);
        foreach (var overload in overloads)
        {
            var attribute = overload.GetAttributes().Single(a => a.AttributeClass!.Name == "ExperimentalAttribute");
            await Assert.That((string)attribute.ConstructorArguments[0].Value!).IsEqualTo(id);
        }
    }

    private static CSharpCompilation Compile(string source)
    {
        var paths = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator);
        return CSharpCompilation.Create("PreviewConsumer", [CSharpSyntaxTree.ParseText(source)],
            paths.Select(static path => MetadataReference.CreateFromFile(path)),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
    }
}
