using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Relayer.SourceGenerators;

namespace Relayer.SourceGenerators.Tests;

public class HandlerGeneratorTests
{
    private const string Handlers = """
        using Relayer;
        using System.Threading;
        using System.Threading.Tasks;
        namespace Example;
        public sealed partial class Handler : IHandler<int>, IBatchHandler<string>
        {
            ValueTask IHandler<int>.HandleAsync(int message, MessageContext context, CancellationToken ct) => default;
            ValueTask<BatchResult> IBatchHandler<string>.HandleAsync(Batch<string> batch, CancellationToken ct) => new(BatchResult.Success);
        }
        public sealed partial class Handler { }
        public abstract class Base : IHandler<int>
        {
            public ValueTask HandleAsync(int message, MessageContext context, CancellationToken ct) => default;
        }
        internal sealed class Derived : Base { }
        """;

    [Test]
    public async Task Generated_source_matches_snapshot_and_compiles()
    {
        var driver = Run(CreateCompilation(Handlers), out _);
        var actual = Generated(driver);
        var expected = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Snapshots", "Handlers.verified.txt"));
        await Assert.That(actual.ReplaceLineEndings("\n")).IsEqualTo(expected.ReplaceLineEndings("\n"));
    }

    [Test]
    public async Task Unrelated_edit_caches_models_and_source_output()
    {
        var unrelated = CSharpSyntaxTree.ParseText("class Unrelated { int Value => 1; }");
        var compilation = CreateCompilation(Handlers).AddSyntaxTrees(unrelated);
        var driver = Run(compilation, out _);
        var updated = compilation.ReplaceSyntaxTree(unrelated, CSharpSyntaxTree.ParseText("class Unrelated { int Value => 2; }"));
        var rerun = Run(updated, out _, driver);
        var result = rerun.GetRunResult().Results.Single();

        await Assert.That(Generated(rerun)).IsEqualTo(Generated(driver));
        await Assert.That(result.TrackedSteps["HandlerModels"].SelectMany(s => s.Outputs)
            .All(o => o.Reason is IncrementalStepRunReason.Cached or IncrementalStepRunReason.Unchanged)).IsTrue();
        await Assert.That(result.TrackedOutputSteps.SelectMany(s => s.Value).SelectMany(s => s.Outputs)
            .All(o => o.Reason == IncrementalStepRunReason.Cached)).IsTrue();
    }

    [Test]
    public async Task Handler_edit_invalidates_output_and_removal_removes_registration()
    {
        var compilation = CreateCompilation(Handlers);
        var originalTree = compilation.SyntaxTrees.Single();
        var first = Run(compilation, out _);
        var renamed = compilation.ReplaceSyntaxTree(originalTree, CSharpSyntaxTree.ParseText(Handlers.Replace("Derived", "Renamed")));
        var second = Run(renamed, out _, first);
        var removed = renamed.ReplaceSyntaxTree(renamed.SyntaxTrees.Single(), CSharpSyntaxTree.ParseText("class Unrelated { }"));
        var third = Run(removed, out _, second);

        await Assert.That(Generated(second)).Contains("global::Example.Renamed");
        await Assert.That(Generated(second)).DoesNotContain("global::Example.Derived");
        await Assert.That(Generated(third)).DoesNotContain("Dispatcher0");
    }

    [Test]
    public async Task Handler_body_edit_keeps_generated_output_cached()
    {
        var compilation = CreateCompilation(Handlers);
        var driver = Run(compilation, out _);
        var updated = compilation.ReplaceSyntaxTree(compilation.SyntaxTrees.Single(),
            CSharpSyntaxTree.ParseText(Handlers.Replace("=> default;", "=> ValueTask.CompletedTask;")));
        var rerun = Run(updated, out _, driver);
        await Assert.That(Generated(rerun)).IsEqualTo(Generated(driver));
        await Assert.That(rerun.GetRunResult().Results.Single().TrackedOutputSteps
            .SelectMany(s => s.Value).SelectMany(s => s.Outputs)
            .All(o => o.Reason == IncrementalStepRunReason.Cached)).IsTrue();
    }

    [Test]
    public async Task Generic_registration_helpers_do_not_emit_unbound_nested_type_parameters()
    {
        var compilation = CreateCompilation("""
            using Relayer;
            using System.Collections.Generic;
            using System.Threading;
            using System.Threading.Tasks;
            class Generic<T> : IHandler<T>
            {
                public ValueTask HandleAsync(T m, MessageContext c, CancellationToken ct) => default;
            }
            class Concrete : Generic<int> { }
            static class Registration
            {
                public static void Add<T>(IRelayerBuilder builder)
                {
                    builder.Handler<Generic<List<T>>>();
                    builder.Handler<Generic<T[]>>();
                }
            }
            """);
        var driver = Run(compilation, out _);
        await Assert.That(Generated(driver)).DoesNotContain("List<T>");
        await Assert.That(Generated(driver)).DoesNotContain("T[]");
    }

    [Test]
    public async Task Discovery_skips_inaccessible_file_local_abstract_and_open_generic_handlers()
    {
        var compilation = CreateCompilation("""
            using Relayer;
            using System.Threading;
            using System.Threading.Tasks;
            abstract class Generic<T> : IHandler<T>
            {
                public ValueTask HandleAsync(T m, MessageContext c, CancellationToken ct) => default;
            }
            class Open<T> : Generic<T> { }
            file class FileHandler : Generic<int> { }
            class Container
            {
                private class Hidden : Generic<int> { }
            }
            """);
        var driver = Run(compilation, out _);
        await Assert.That(Generated(driver)).DoesNotContain("Dispatcher0");
    }

    [Test]
    public async Task Inaccessible_message_types_report_actionable_diagnostic()
    {
        var compilation = CreateCompilation("""
            using Relayer;
            using System.Threading;
            using System.Threading.Tasks;
            public class Container
            {
                private sealed class HiddenMessage { }
                public sealed class Handler : IHandler<HiddenMessage>
                {
                    ValueTask IHandler<HiddenMessage>.HandleAsync(HiddenMessage m, MessageContext c, CancellationToken ct) => default;
                }
            }
            """);
        await Assert.That(compilation.GetDiagnostics().Any(d => d.Severity == DiagnosticSeverity.Error)).IsFalse();
        GeneratorDriver driver = CSharpGeneratorDriver.Create(new HandlerGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);
        await Assert.That(diagnostics.Single().Id).IsEqualTo("RLYH001");
        await Assert.That(diagnostics.Single().GetMessage()).Contains("Make the message type assembly-accessible");
        await Assert.That(output.GetDiagnostics().Any(d => d.Severity == DiagnosticSeverity.Error)).IsFalse();
    }

    [Test]
    public async Task Dynamic_contract_uses_object_without_runtime_binding()
    {
        var driver = Run(CreateCompilation("""
            using Relayer;
            using System.Threading;
            using System.Threading.Tasks;
            public sealed class Handler<T> : IHandler<T>
            {
                public ValueTask HandleAsync(T m, MessageContext c, CancellationToken ct) => default;
            }
            public static class Registration
            {
                public static void Add(IRelayerBuilder builder) => builder.Handler<Handler<dynamic>>();
            }
            """), out _);
        await Assert.That(Generated(driver)).Contains("HandlerDescriptor<global::System.Object>");
        await Assert.That(Generated(driver)).DoesNotContain("dynamic message");
        await Assert.That(Generated(driver)).Contains("IHandler<global::System.Object>");
    }

    [Test]
    public async Task Referenced_handler_can_be_registered_explicitly_without_assembly_discovery()
    {
        var external = CreateCompilation(Handlers).WithAssemblyName("External");
        using var stream = new MemoryStream();
        var emitted = external.Emit(stream);
        await Assert.That(emitted.Success).IsTrue();
        var compilation = CreateCompilation("""
            using Relayer;
            public static class Registration
            {
                public static void Add(IRelayerBuilder builder) => builder.Handler<Example.Handler>();
            }
            """).AddReferences(MetadataReference.CreateFromImage(stream.ToArray()));
        var driver = Run(compilation, out _);
        var generated = Generated(driver);
        await Assert.That(generated).Contains("TryAddScoped<global::Example.Handler>");
        var discovery = generated[..generated.IndexOf("public static global::Relayer.IRelayerBuilder Handler<", StringComparison.Ordinal)];
        await Assert.That(discovery).DoesNotContain("Handler<global::Example.Handler>(builder)");
    }

    private static CSharpCompilation CreateCompilation(string source)
    {
        var paths = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator);
        var references = paths.Select(static path => MetadataReference.CreateFromFile(path)).ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(IHandler<>).Assembly.Location));
        return CSharpCompilation.Create("GeneratorConsumer", [CSharpSyntaxTree.ParseText(source)], references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));
    }

    private static GeneratorDriver Run(CSharpCompilation compilation, out Compilation output, GeneratorDriver? driver = null)
    {
        driver ??= CSharpGeneratorDriver.Create([new HandlerGenerator().AsSourceGenerator()],
            driverOptions: new GeneratorDriverOptions(IncrementalGeneratorOutputKind.None, trackIncrementalGeneratorSteps: true));
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out output, out var diagnostics);
        var errors = diagnostics.Concat(output.GetDiagnostics()).Where(d => d.Severity == DiagnosticSeverity.Error).ToArray();
        if (errors.Length != 0)
        {
            throw new InvalidOperationException(string.Join(Environment.NewLine, errors.Select(static d => d.ToString())));
        }
        return driver;
    }

    private static string Generated(GeneratorDriver driver)
        => string.Join("\n", driver.GetRunResult().Results.Single().GeneratedSources.Select(s => s.SourceText.ToString()));
}
