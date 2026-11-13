using System.Reflection;
using System.Runtime.Loader;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Relayer.SourceGenerators;

namespace Relayer.SourceGenerators.Tests;

public class MessageTypeGeneratorTests
{
    [Test]
    public async Task Inaccessible_messages_are_diagnosed_without_generated_source_errors()
    {
        var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator).Select(static p => MetadataReference.CreateFromFile(p)).ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(MessageAttribute).Assembly.Location));
        var tree = CSharpSyntaxTree.ParseText("""
            public class Container
            {
                [Relayer.Message("private")] private sealed record Hidden(int Value);
                [Relayer.Message("protected")] protected sealed record Protected(int Value);
                private class HiddenContainer
                {
                    [Relayer.Message("container")] public sealed record Nested(int Value);
                }
                [Relayer.Message("internal")] internal sealed record ValidNested(int Value);
            }
            [Relayer.Message("local")] file sealed record Local(int Value);
            file class LocalContainer
            {
                [Relayer.Message("local-container")] public sealed record Nested(int Value);
            }
            [Relayer.Message("public")] public sealed record ValidPublic(int Value);
            [Relayer.Message("internal-root")] internal sealed record ValidInternal(int Value);
            """);
        var compilation = CSharpCompilation.Create("AccessibleMessages", [tree], references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        GeneratorDriver driver = CSharpGeneratorDriver.Create(new MessageTypeGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);
        await Assert.That(diagnostics.Length).IsEqualTo(5);
        foreach (var diagnostic in diagnostics)
        {
            await Assert.That(diagnostic.Id).IsEqualTo("RLYM002");
            await Assert.That(diagnostic.Location.SourceSpan.Length > 0).IsTrue();
            await Assert.That(diagnostic.Severity).IsEqualTo(DiagnosticSeverity.Error);
            await Assert.That(diagnostic.Descriptor.HelpLinkUri)
                .IsEqualTo("https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlym002");
        }
        await Assert.That(string.Join("\n", output.GetDiagnostics()
            .Where(d => d.Severity == DiagnosticSeverity.Error))).IsEqualTo("");
        var sources = string.Join("\n", driver.GetRunResult().GeneratedTrees.Select(t => t.ToString()));
        await Assert.That(sources).Contains("Add<global::Container.ValidNested>");
        await Assert.That(sources).Contains("Add<global::ValidPublic>");
        await Assert.That(sources).Contains("Add<global::ValidInternal>");
    }

    [Test]
    public async Task Static_messages_are_diagnosed_without_generated_source_errors()
    {
        var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator).Select(static p => MetadataReference.CreateFromFile(p)).ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(MessageAttribute).Assembly.Location));
        var tree = CSharpSyntaxTree.ParseText("""
            [Relayer.Message("static")]
            public static class InvalidMessage { }
            [Relayer.Message("ordinary")]
            public sealed record OrdinaryMessage(int Value);
            public static class Container
            {
                [Relayer.Message("nested")]
                public sealed record NestedMessage(int Value);
            }
            """);
        var compilation = CSharpCompilation.Create("StaticMessages", [tree], references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        GeneratorDriver driver = CSharpGeneratorDriver.Create(new MessageTypeGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);
        await Assert.That(diagnostics.Length).IsEqualTo(1);
        var diagnostic = diagnostics[0];
        await Assert.That(diagnostic.Id).IsEqualTo("RLYM004");
        await Assert.That(diagnostic.Severity).IsEqualTo(DiagnosticSeverity.Error);
        await Assert.That(diagnostic.Location.GetLineSpan().Path).IsEqualTo(tree.FilePath);
        await Assert.That(tree.GetText().ToString(diagnostic.Location.SourceSpan))
            .IsEqualTo("[Relayer.Message(\"static\")]\npublic static class InvalidMessage { }");
        await Assert.That(diagnostic.Descriptor.HelpLinkUri)
            .IsEqualTo("https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlym004");
        await Assert.That(diagnostic.GetMessage()).Contains("non-static");
        await Assert.That(string.Join("\n", output.GetDiagnostics()
            .Where(d => d.Severity == DiagnosticSeverity.Error))).IsEqualTo("");
        var sources = string.Join("\n", driver.GetRunResult().GeneratedTrees.Select(t => t.ToString()));
        await Assert.That(sources).DoesNotContain("global::InvalidMessage");
        await Assert.That(sources).Contains("Add<global::OrdinaryMessage>");
        await Assert.That(sources).Contains("Add<global::Container.NestedMessage>");
        await Assert.That(sources).Contains("typeof(global::OrdinaryMessage)");
        await Assert.That(sources).Contains("typeof(global::Container.NestedMessage)");
    }

    [Test]
    public async Task Pinned_json_generator_is_embedded_and_generates_compilable_metadata_after_reload()
    {
        // The pinned implementation must travel with each generator binary, rather
        // than being resolved by name from the compiler server's analyzer dependencies.
        await Assert.That(typeof(MessageTypeGenerator).Assembly.GetReferencedAssemblies()
            .Any(a => a.Name == "System.Text.Json.SourceGeneration")).IsFalse();
        await Assert.That(typeof(MessageTypeGenerator).Assembly.GetManifestResourceNames())
            .Contains("Relayer.System.Text.Json.SourceGeneration.dll");

        var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator).Select(static p => MetadataReference.CreateFromFile(p)).ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(MessageAttribute).Assembly.Location));
        var compilation = CSharpCompilation.Create("MessageConsumer",
            [CSharpSyntaxTree.ParseText("""
                [Relayer.Message("order")]
                public sealed record Order(int Id);
                """)], references, new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var competingGenerator = CSharpCompilation.Create("System.Text.Json.SourceGeneration",
            [CSharpSyntaxTree.ParseText("""
                [assembly: System.Reflection.AssemblyVersion("11.0.0.0")]
                namespace System.Text.Json.SourceGeneration;
                public sealed class JsonSourceGenerator : Microsoft.CodeAnalysis.IIncrementalGenerator
                {
                    public void Initialize(Microsoft.CodeAnalysis.IncrementalGeneratorInitializationContext context)
                        => throw new System.InvalidOperationException("Competing generator selected");
                }
                """)], references, new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        using var competingImage = new MemoryStream();
        await Assert.That(competingGenerator.Emit(competingImage).Success).IsTrue();

        // Model a compiler analyzer loader that resolves the dependency to a newer
        // implementation, including after reloading the rebuilt Relayer assembly.
        for (var reload = 0; reload < 2; reload++)
        {
            var loader = new CompetingGeneratorLoadContext(competingImage.ToArray());
            using var image = File.OpenRead(typeof(MessageTypeGenerator).Assembly.Location);
            var assembly = loader.LoadFromStream(image);
            var generator = (IIncrementalGenerator)Activator.CreateInstance(
                assembly.GetType(typeof(MessageTypeGenerator).FullName!)!)!;
            GeneratorDriver driver = CSharpGeneratorDriver.Create(generator);
            driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);
            await Assert.That(diagnostics.Any(d => d.Severity == DiagnosticSeverity.Error)).IsFalse();
            var errors = output.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error);
            await Assert.That(string.Join("\n", errors)).IsEqualTo("");
            var sources = string.Join("\n", driver.GetRunResult().GeneratedTrees.Select(t => t.ToString()));
            await Assert.That(sources).Contains("System.Text.Json.SourceGeneration");
            await Assert.That(sources).DoesNotContain("TypeClassifierFactory");
        }
    }

    [Test]
    [Arguments("public sealed record Order<T>(T Value);", "Order<T>")]
    [Arguments("public class Container<T> { [Relayer.Message(\"invalid\")] public sealed record Order(int Value); }", "Container<T>.Order")]
    [Arguments("public class Outer<T> { public class Inner { [Relayer.Message(\"invalid\")] public sealed record Order(int Value); } }", "Outer<T>.Inner.Order")]
    public async Task Open_generic_messages_report_diagnostic_without_emitting_invalid_references(string declaration, string typeName)
    {
        var source = (declaration.StartsWith("public sealed", StringComparison.Ordinal)
            ? "[Relayer.Message(\"invalid\")] " : "") + declaration + """

            [Relayer.Message("concrete")]
            public sealed record Concrete(int Value);
            """;
        var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator).Select(static p => MetadataReference.CreateFromFile(p)).ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(MessageAttribute).Assembly.Location));
        var tree = CSharpSyntaxTree.ParseText(source);
        var compilation = CSharpCompilation.Create("GenericMessageConsumer", [tree], references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        GeneratorDriver driver = CSharpGeneratorDriver.Create(new MessageTypeGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);

        await Assert.That(diagnostics.Length).IsEqualTo(1);
        var diagnostic = diagnostics[0];
        await Assert.That(diagnostic.Id).IsEqualTo("RLYM001");
        await Assert.That(diagnostic.Severity).IsEqualTo(DiagnosticSeverity.Error);
        await Assert.That(diagnostic.Location.GetLineSpan()).IsEqualTo(tree.GetLineSpan(diagnostic.Location.SourceSpan));
        await Assert.That(tree.GetText().ToString(diagnostic.Location.SourceSpan)).Contains("record Order");
        await Assert.That(diagnostic.GetMessage()).Contains(typeName);
        await Assert.That(diagnostic.Descriptor.HelpLinkUri)
            .IsEqualTo("https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlym001");
        await Assert.That(string.Join("\n", output.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error)))
            .IsEqualTo("");
        var generated = string.Join("\n", driver.GetRunResult().GeneratedTrees.Select(t => t.ToString()));
        await Assert.That(generated).DoesNotContain(typeName);
        await Assert.That(generated).Contains("Add<global::Concrete>(\"concrete\", 1)");
        await Assert.That(generated).Contains("typeof(global::Concrete)");
        using var image = new MemoryStream();
        await Assert.That(output.Emit(image).Success).IsTrue();
    }

    [Test]
    public async Task Unchanged_open_generic_declarations_reuse_incremental_models_after_tree_replacement()
    {
        const string source = "[Relayer.Message(\"invalid\")] public sealed record Order<T>(T Value);";
        var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator).Select(static p => MetadataReference.CreateFromFile(p)).ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(MessageAttribute).Assembly.Location));
        var tree = CSharpSyntaxTree.ParseText(source, path: "Message.cs");
        var compilation = CSharpCompilation.Create("IncrementalMessageConsumer", [tree], references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            [new MessageTypeGenerator().AsSourceGenerator()],
            driverOptions: new GeneratorDriverOptions(IncrementalGeneratorOutputKind.None, trackIncrementalGeneratorSteps: true));
        driver = driver.RunGenerators(compilation);
        compilation = compilation.ReplaceSyntaxTree(tree, CSharpSyntaxTree.ParseText(source, path: "Message.cs"));
        driver = driver.RunGenerators(compilation);

        var result = driver.GetRunResult().Results.Single();
        var output = result.TrackedSteps["MessageDeclarations"].Single().Outputs.Single();
        await Assert.That(output.Reason).IsEqualTo(IncrementalStepRunReason.Cached);
        await Assert.That(result.Diagnostics.Single().Location.GetLineSpan().Path).IsEqualTo("Message.cs");
    }

    [Test]
    [Arguments("orders.placed")]
    [Arguments("orders\nplaced")]
    [Arguments("orders\rplaced")]
    [Arguments("orders\r\nplaced")]
    [Arguments("orders\u0085placed")]
    [Arguments("orders\u2028placed")]
    [Arguments("orders\u2029placed")]
    [Arguments("orders\"placed")]
    [Arguments("orders\\placed")]
    [Arguments("orders\0\tplaced")]
    public async Task Generated_registration_preserves_message_name(string name)
    {
        var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator).Select(static p => MetadataReference.CreateFromFile(p)).ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(MessageAttribute).Assembly.Location));
        var source = $$"""
            using Relayer;
            using Microsoft.Extensions.DependencyInjection;
            [Message({{SymbolDisplay.FormatLiteral(name, quote: true)}}, Version = 2)]
            public sealed record Order(int Id);
            public static class Registration
            {
                public static MessageType Register()
                {
                    MessageType identity = default;
                    new ServiceCollection().AddRelayer(builder =>
                    {
                        builder.AddMessageTypesFromThisAssembly();
                        if (!builder.MessageTypes.TryGetMessageType(typeof(Order), out identity))
                            throw new System.InvalidOperationException("Message was not registered");
                    });
                    return identity;
                }
            }
            """;
        var compilation = CSharpCompilation.Create("MessageConsumer_" + Guid.NewGuid().ToString("N"),
            [CSharpSyntaxTree.ParseText(source)], references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        GeneratorDriver driver = CSharpGeneratorDriver.Create(new MessageTypeGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);
        await Assert.That(string.Join("\n", diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error)))
            .IsEqualTo("");
        using var image = new MemoryStream();
        var result = output.Emit(image);
        await Assert.That(string.Join("\n", result.Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error)))
            .IsEqualTo("");
        await Assert.That(result.Success).IsTrue();

        var loader = new AssemblyLoadContext(compilation.AssemblyName, isCollectible: true);
        try
        {
            image.Position = 0;
            var assembly = loader.LoadFromStream(image);
            var identity = (MessageType)assembly.GetType("Registration")!.GetMethod("Register")!.Invoke(null, null)!;
            await Assert.That(identity.Name).IsEqualTo(name);
            await Assert.That(identity.Version).IsEqualTo(2);
            if (name == "orders.placed")
            {
                var registration = driver.GetRunResult().GeneratedTrees
                    .Single(t => t.FilePath.EndsWith("RelayerGeneratedMessageTypes.g.cs", StringComparison.Ordinal));
                await Assert.That(registration.ToString()).Contains("Add<global::Order>(\"orders.placed\", 2)");
            }
        }
        finally
        {
            loader.Unload();
        }
    }

    private sealed class CompetingGeneratorLoadContext(byte[] competingImage) : AssemblyLoadContext
    {
        protected override Assembly? Load(AssemblyName assemblyName) =>
            assemblyName.Name == "System.Text.Json.SourceGeneration"
                ? LoadFromStream(new MemoryStream(competingImage))
                : null;
    }
}
