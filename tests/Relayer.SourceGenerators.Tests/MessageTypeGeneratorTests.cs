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

    private sealed class CompetingGeneratorLoadContext(byte[] competingImage) : AssemblyLoadContext
    {
        protected override Assembly? Load(AssemblyName assemblyName) =>
            assemblyName.Name == "System.Text.Json.SourceGeneration"
                ? LoadFromStream(new MemoryStream(competingImage))
                : null;
    }
}
