using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Relayer.CodeFixes;
using Relayer.SourceGenerators;

namespace Relayer.SourceGenerators.Tests;

public class ConfigurationAnalyzerTests
{
    private const string Imports = """
        using Relayer;
        using System.Threading;
        using System.Threading.Tasks;
        using Broker = Relayer.TransportBuilder<object, object, object>;

        """;
    private const string Handler = """
        [Message("order")] public record Order;
        public class H : IHandler<Order>
        {
            public ValueTask HandleAsync(Order message, MessageContext context, CancellationToken ct) => default;
        }
        """;

    [Test]
    public async Task Missing_identity_in_route_and_handler_reports_once_at_message_declaration()
    {
        var source = Imports + Handler.Replace("[Message(\"order\")] ", "") + """
            static class Setup { public static void Add(Broker broker, IRelayerBuilder builder)
                { broker.Send<Order>("orders"); builder.Handler<H>(); } }
            """;
        var diagnostics = await Analyze(source);
        await Assert.That(Ids(diagnostics)).IsEqualTo("RLY001");
        await Assert.That(source.Substring(diagnostics[0].Location.SourceSpan.Start, diagnostics[0].Location.SourceSpan.Length)).IsEqualTo("Order");
        await Assert.That(diagnostics[0].AdditionalLocations.Count).IsEqualTo(1);
        await Assert.That(diagnostics[0].Severity).IsEqualTo(DiagnosticSeverity.Warning);
    }

    [Test]
    [Arguments(true, "")]
    [Arguments(false, "RLY001")]
    public async Task Tombstone_handlers_require_the_original_message_identity(bool identity, string expected)
    {
        var source = Imports + Handler.Replace("IHandler<Order>", "IHandler<Tombstone<Order>>")
            .Replace("HandleAsync(Order message", "HandleAsync(Tombstone<Order> message") +
            "static class Setup { public static void Add(IRelayerBuilder builder) => builder.Handler<H>(); }";
        if (!identity) source = source.Replace("[Message(\"order\")] ", "");
        await Verify(source, expected);
    }

    [Test]
    public async Task Route_only_is_analyzed_and_unrelated_Send_is_ignored()
    {
        await Verify(Imports + """
            public record Order;
            static class Setup { public static void Add(Broker broker) => broker.Send<Order>("orders"); }
            """, "RLY001");
        await Verify(Imports + """
            public record Order;
            class Other { public void Send<T>(string destination) {} public void Add() => Send<Order>("orders"); }
            """, "");
    }

    [Test]
    [Arguments("1", "RLY002,RLY002")]
    [Arguments("2", "")]
    public async Task Duplicate_identity_includes_default_version_and_partial_types(string version, string expected)
    {
        var diagnostics = await Analyze(Imports + $$"""
            [Message("order")] public partial record First;
            public partial record First;
            [Message("order", Version = {{version}})] public record Second;
            [Message("Order")] public record CaseSensitive;
            """);
        await Assert.That(Ids(diagnostics)).IsEqualTo(expected);
        foreach (var diagnostic in diagnostics)
        {
            await Assert.That(diagnostic.Severity).IsEqualTo(DiagnosticSeverity.Error);
            await Assert.That(diagnostic.GetMessage()).Contains("'order' version 1");
            await Assert.That(diagnostic.Location.IsInSource).IsTrue();
        }
    }

    [Test]
    [Arguments("", "RLY003")]
    [Arguments("builder.Handler<H>();", "")]
    [Arguments("RelayerGeneratedHandlers.Handler<H>(builder);", "")]
    [Arguments("builder.AddHandlersFromThisAssembly();", "")]
    [Arguments("RelayerGeneratedHandlers.AddHandlersFromThisAssembly(builder);", "")]
    public async Task Registration_is_compilation_wide_and_supports_static_and_extension_calls(string registration, string expected)
    {
        await Verify(Imports + Handler + $$"""
            static class Setup { public static void Add(IRelayerBuilder builder) { {{registration}} } }
            """, expected);
    }

    [Test]
    public async Task Unrelated_registration_names_do_not_register_handlers()
    {
        await Verify(Imports + Handler + """
            static class Other
            {
                public static void Handler<T>() { }
                public static void AddHandlersFromThisAssembly() { }
                public static void Add() { Handler<H>(); AddHandlersFromThisAssembly(); }
            }
            """, "RLY003");
    }

    [Test]
    public async Task Batch_inherited_and_explicit_contracts_are_checked()
    {
        await Verify(Imports + """
            public record Order;
            public abstract class Base : IBatchHandler<Order>
            {
                ValueTask<BatchResult> IBatchHandler<Order>.HandleAsync(Batch<Order> b, CancellationToken ct) => new(BatchResult.Success);
            }
            public class Concrete : Base { }
            public abstract class Abstract : Base { }
            public class Open<T> : IHandler<T>
            {
                public ValueTask HandleAsync(T message, MessageContext context, CancellationToken ct) => default;
            }
            """, "RLY001,RLY003");
    }

    [Test]
    public async Task Inaccessible_and_file_local_handlers_are_not_registration_candidates()
    {
        await Verify(Imports + """
            [Message("order")] public record Order;
            class Container
            {
                private class Hidden : IHandler<Order>
                { public ValueTask HandleAsync(Order m, MessageContext c, CancellationToken ct) => default; }
            }
            file class HiddenFile : IHandler<Order>
            { public ValueTask HandleAsync(Order m, MessageContext c, CancellationToken ct) => default; }
            """, "");
    }

    [Test]
    [Arguments("_ => { }", "RLY004")]
    [Arguments("delegate(ConsumerBuilder<object> c) { }", "RLY004")]
    [Arguments("c => c.Concurrency(8)", "RLY004")]
    [Arguments("c => c.Concurrency(8).Batch(10, System.TimeSpan.Zero)", "RLY004")]
    [Arguments("c => c.Handler<H>()", "")]
    [Arguments("c => { var alias = c; alias.Handler<H>(); }", "")]
    [Arguments("c => Configure(c)", "")]
    [Arguments("Configure", "")]
    [Arguments("c => { System.Action register = () => c.Handler<H>(); register(); }", "")]
    public async Task Consumers_warn_only_for_visibly_missing_registration(string callback, string expected)
    {
        await Verify(Imports + Handler + $$"""
            static class Setup
            {
                public static void Add(Broker broker, IRelayerBuilder builder)
                {
                    builder.Handler<H>();
                    broker.Consume("orders", {{callback}});
                }
                static void Configure(ConsumerBuilder<object> c) => c.Handler<H>();
            }
            """, expected);
    }

    [Test]
    public async Task Registration_on_another_consumer_does_not_hide_empty_consumer()
    {
        await Verify(Imports + Handler + """
            static class Setup { public static void Add(Broker broker)
                { broker.Consume("good", c => c.Handler<H>()); broker.Consume("empty", _ => {}); } }
            """, "RLY004");
    }

    [Test]
    public async Task Native_options_without_handler_are_reported()
    {
        await Verify(Imports + """
            public class Options { public int Prefetch { get; set; } }
            static class Setup
            {
                public static void Add(TransportBuilder<object, object, Options> broker)
                    => broker.Consume("orders", c => { c.Options.Prefetch = 10; });
            }
            """, "RLY004");
    }

    [Test]
    public async Task Referenced_messages_are_checked_without_offering_an_uneditable_fix()
    {
        var diagnostics = await Analyze(Imports + """
            static class Setup { public static void Add(Broker broker) => broker.Send<int>("orders"); }
            """);
        await Assert.That(Ids(diagnostics)).IsEqualTo("RLY001");
        await Assert.That(diagnostics[0].AdditionalLocations.Count).IsEqualTo(0);
    }

    [Test]
    public async Task External_type_uses_each_have_a_location_so_pragmas_are_deterministic()
    {
        var source = Imports + """
            static class Setup
            {
            #pragma warning disable RLY001
                public static void Suppressed(Broker broker) => broker.Send<int>("suppressed");
            #pragma warning restore RLY001
                public static void Visible(Broker broker) => broker.Send<int>("visible");
            }
            """;
        var diagnostics = await Analyze(source);
        await Assert.That(Ids(diagnostics)).IsEqualTo("RLY001");
        await Assert.That(source.Substring(diagnostics[0].Location.SourceSpan.Start, diagnostics[0].Location.SourceSpan.Length))
            .Contains("visible");
    }

    [Test]
    public async Task Generated_code_is_ignored()
    {
        var source = "// <auto-generated/>\n" + Imports + Handler;
        var diagnostics = await Analyze(source);
        await Assert.That(diagnostics.Length).IsEqualTo(0);
    }

    [Test]
    public async Task Attribute_identity_uses_symbols_not_short_names()
    {
        await Verify(Imports + """
            namespace Example
            {
                public sealed class MessageAttribute(string name) : System.Attribute { }
                [Message("order")] public record Order;
                static class Setup { public static void Add(Broker broker) => broker.Send<Order>("orders"); }
            }
            """, "RLY001");
    }

    [Test]
    public async Task Referenced_handler_is_checked_on_explicit_registration()
    {
        var external = Compile(Imports + Handler.Replace("[Message(\"order\")] ", "")).WithAssemblyName("External");
        using var stream = new MemoryStream();
        await Assert.That(external.Emit(stream).Success).IsTrue();
        var compilation = Compile(Imports + """
            static class Setup { public static void Add(IRelayerBuilder builder) => builder.Handler<H>(); }
            """).AddReferences(MetadataReference.CreateFromImage(stream.ToArray()));
        GeneratorDriver driver = CSharpGeneratorDriver.Create(new HandlerGenerator());
        driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out _);
        var diagnostics = await output.WithAnalyzers([new ConfigurationAnalyzer()]).GetAnalyzerDiagnosticsAsync();
        await Assert.That(Ids(diagnostics)).IsEqualTo("RLY001");
        await Assert.That(diagnostics[0].AdditionalLocations.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Editorconfig_suppression_is_respected()
    {
        var compilation = Compile(Imports + Handler).WithOptions(new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
            .WithSpecificDiagnosticOptions(new Dictionary<string, ReportDiagnostic> { ["RLY003"] = ReportDiagnostic.Suppress }));
        var diagnostics = await compilation.WithAnalyzers([new ConfigurationAnalyzer()]).GetAnalyzerDiagnosticsAsync();
        await Assert.That(diagnostics.Length).IsEqualTo(0);
    }

    [Test]
    [Arguments("record Order;", "record Order;")]
    [Arguments("partial class Order {}", "partial class Order")]
    [Arguments("struct Order {}", "struct Order")]
    [Arguments("record struct Order;", "record struct Order;")]
    public async Task Code_fix_adds_attribute_preserves_trivia_and_removes_warning(string declaration, string expectedDeclaration)
    {
        using var workspace = new AdhocWorkspace();
        var project = workspace.AddProject("Fix", LanguageNames.CSharp).WithMetadataReferences(References())
            .WithCompilationOptions(new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        var document = project.AddDocument("Message.cs", Imports + "// Keep this comment\npublic " + declaration + """

            static class Setup { public static void Add(Broker broker) => broker.Send<Order>("orders"); }
            """);
        var compilation = (CSharpCompilation)(await document.Project.GetCompilationAsync())!;
        var diagnostic = (await compilation.WithAnalyzers([new ConfigurationAnalyzer()]).GetAnalyzerDiagnosticsAsync()).Single();
        var actions = new List<CodeAction>();
        await new MessageAttributeCodeFixProvider().RegisterCodeFixesAsync(new CodeFixContext(document, diagnostic,
            (action, _) => actions.Add(action), CancellationToken.None));
        await Assert.That(actions.Count).IsEqualTo(1);
        var operation = (await actions[0].GetOperationsAsync(CancellationToken.None)).OfType<ApplyChangesOperation>().Single();
        var fixedDocument = operation.ChangedSolution.GetDocument(document.Id)!;
        var fixedSource = (await fixedDocument.GetTextAsync()).ToString();
        await Assert.That(fixedSource).Contains("// Keep this comment");
        await Assert.That(fixedSource).Contains("[global::Relayer.MessageAttribute(\"Order\")]");
        await Assert.That(fixedSource).Contains(expectedDeclaration);
        var fixedCompilation = (await fixedDocument.Project.GetCompilationAsync())!;
        await Assert.That(string.Join("\n", fixedCompilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error))).IsEqualTo("");
        await Assert.That((await fixedCompilation.WithAnalyzers([new ConfigurationAnalyzer()]).GetAnalyzerDiagnosticsAsync()).Length).IsEqualTo(0);
    }

    [Test]
    public async Task Code_fix_targets_message_file_instead_of_route_file()
    {
        using var workspace = new AdhocWorkspace();
        var project = workspace.AddProject("CrossFileFix", LanguageNames.CSharp).WithMetadataReferences(References())
            .WithCompilationOptions(new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        var message = project.AddDocument("Message.cs", "public record Order;");
        var setup = message.Project.AddDocument("Setup.cs", Imports + """
            static class Setup { public static void Add(Broker broker) => broker.Send<Order>("orders"); }
            """);
        var compilation = (await setup.Project.GetCompilationAsync())!;
        var diagnostic = (await compilation.WithAnalyzers([new ConfigurationAnalyzer()]).GetAnalyzerDiagnosticsAsync()).Single();
        var actions = new List<CodeAction>();
        var originalMessage = setup.Project.GetDocument(message.Id)!;
        await new MessageAttributeCodeFixProvider().RegisterCodeFixesAsync(new CodeFixContext(originalMessage, diagnostic,
            (action, _) => actions.Add(action), CancellationToken.None));
        var operation = (await actions.Single().GetOperationsAsync(CancellationToken.None)).OfType<ApplyChangesOperation>().Single();
        await Assert.That((await operation.ChangedSolution.GetDocument(message.Id)!.GetTextAsync()).ToString())
            .Contains("[global::Relayer.MessageAttribute(\"Order\")]");
        await Assert.That((await operation.ChangedSolution.GetDocument(setup.Id)!.GetTextAsync()).ToString())
            .IsEqualTo((await setup.GetTextAsync()).ToString());
    }

    private static async Task Verify(string source, string expected) =>
        await Assert.That(Ids(await Analyze(source))).IsEqualTo(expected);

    private static string Ids(ImmutableArray<Diagnostic> diagnostics) =>
        string.Join(",", diagnostics.Select(static d => d.Id).Order(StringComparer.Ordinal));

    private static async Task<ImmutableArray<Diagnostic>> Analyze(string source)
    {
        GeneratorDriver driver = CSharpGeneratorDriver.Create(new HandlerGenerator());
        driver.RunGeneratorsAndUpdateCompilation(Compile(source), out var output, out var generatorDiagnostics);
        var errors = generatorDiagnostics.Concat(output.GetDiagnostics()).Where(static d => d.Severity == DiagnosticSeverity.Error);
        await Assert.That(string.Join("\n", errors)).IsEqualTo("");
        var diagnostics = await output.WithAnalyzers([new ConfigurationAnalyzer()]).GetAnalyzerDiagnosticsAsync();
        await Assert.That(diagnostics.Any(static d => d.Id == "AD0001")).IsFalse();
        return diagnostics;
    }

    private static CSharpCompilation Compile(string source) => CSharpCompilation.Create("AnalyzerConsumer",
        [CSharpSyntaxTree.ParseText(source)], References(), new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

    private static IEnumerable<MetadataReference> References() =>
        ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator)
        .Append(typeof(IHandler<>).Assembly.Location).Distinct().Select(static path => MetadataReference.CreateFromFile(path));
}
