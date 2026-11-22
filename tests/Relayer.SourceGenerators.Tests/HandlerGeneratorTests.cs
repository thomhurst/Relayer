using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Relayer.Pipeline;
using Relayer.SourceGenerators;

namespace Relayer.SourceGenerators.Tests;

public class HandlerGeneratorTests
{
    [Test]
    [Arguments("System.Obsolete", "CS0612")]
    [Arguments("System.Obsolete(\"Use a current handler.\")", "CS0618")]
    public async Task Obsolete_handlers_compile_without_explicit_registration(string attribute, string diagnosticId)
    {
        var compilation = CreateCompilation($$"""
            using Relayer;
            using System.Threading;
            using System.Threading.Tasks;

            [{{attribute}}]
            public sealed class LegacyHandler : IHandler<int>
            {
                public ValueTask HandleAsync(int message, MessageContext context, CancellationToken token) => default;
            }
            """);
        var driver = Run(compilation.WithOptions(compilation.Options.WithGeneralDiagnosticOption(ReportDiagnostic.Error)),
            out var output);
        await Assert.That(Generated(driver)).Contains("TryAddScoped<global::LegacyHandler>");

        var usage = CSharpSyntaxTree.ParseText("""
            using Relayer;

            public static class Registration
            {
                public static void Add(IConsumerBuilder builder) => builder.Handler<LegacyHandler>();
            }
            """, path: "Registration.cs");
        var diagnostic = output.AddSyntaxTrees(usage).GetDiagnostics().Single(d => d.Severity == DiagnosticSeverity.Error);
        await Assert.That(diagnostic.Id).IsEqualTo(diagnosticId);
        await Assert.That(diagnostic.Location.SourceTree).IsEqualTo(usage);
    }

    [Test]
    public async Task Inherited_saga_registers_one_typed_execution_pool_per_registration()
    {
        const string Source = """
            namespace Relayer.Sagas
            {
                public abstract class Saga<TState,TMessage> : global::Relayer.IHandler<TMessage>
                {
                    public global::System.Threading.Tasks.ValueTask HandleAsync(TMessage message, global::Relayer.MessageContext context,
                        global::System.Threading.CancellationToken cancellationToken) => default;
                }
                public sealed class SagaExecution<TState,TMessage> { }
            }
            namespace Example
            {
                public sealed class State { }
                public abstract class Intermediate<T> : global::Relayer.Sagas.Saga<State,T> { }
                public sealed class Process : Intermediate<int> { }
            }
            """;
        var driver = Run(CreateCompilation(Source), out var output);
        await Assert.That(Generated(driver)).Contains("TryAddSingleton<global::Relayer.Sagas.SagaExecution<global::Example.State, int>>");
        await Assert.That(output.GetDiagnostics().Any(d => d.Severity == DiagnosticSeverity.Error)).IsFalse();
    }

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
    public async Task Root_builder_registration_is_rejected_at_compile_time()
    {
        var compilation = CreateCompilation(Handlers + """

            public static class RootRegistration
            {
                public static void Add(IRelayerBuilder builder)
                {
                    builder.Handler<Handler>();
                    builder.AddHandlersFromThisAssembly();
                }
            }
            """);
        GeneratorDriver driver = CSharpGeneratorDriver.Create(new HandlerGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out _);

        await Assert.That(Generated(driver)).DoesNotContain("IRelayerBuilder");
        await Assert.That(Generated(driver)).DoesNotContain("AddHandlersFromThisAssembly");
        var errors = output.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error).ToArray();
        await Assert.That(errors.Length).IsEqualTo(2);
        await Assert.That(errors.Any(d => d.Id == "CS1929")).IsTrue();
        await Assert.That(errors.Any(d => d.Id == "CS1061")).IsTrue();
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
                public static void Add<T>(IConsumerBuilder builder)
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
        await Assert.That(diagnostics.Single().Descriptor.HelpLinkUri)
            .IsEqualTo("https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlyh001");
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
                public static void Add(IConsumerBuilder builder) => builder.Handler<Handler<dynamic>>();
            }
            """), out _);
        await Assert.That(Generated(driver)).Contains("HandlerDescriptor<global::System.Object>");
        await Assert.That(Generated(driver)).DoesNotContain("dynamic message");
        await Assert.That(Generated(driver)).Contains("IHandler<global::System.Object>");
    }

    [Test]
    [Arguments("IConsumerBuilder", "builder.Handler<Example.Handler>()")]
    [Arguments("ITransportConsumerBuilder", "builder.Consume<Example.Handler>(\"orders\")")]
    public async Task Referenced_handler_can_be_registered_explicitly_without_assembly_discovery(string builderType, string registration)
    {
        var external = CreateCompilation(Handlers).WithAssemblyName("External");
        using var stream = new MemoryStream();
        var emitted = external.Emit(stream);
        await Assert.That(emitted.Success).IsTrue();
        var compilation = CreateCompilation("""
            using Relayer;
            public static class Registration
            {
                public static void Add(BUILDER builder) => REGISTRATION;
            }
            """.Replace("BUILDER", builderType).Replace("REGISTRATION", registration)).AddReferences(MetadataReference.CreateFromImage(stream.ToArray()));
        var driver = Run(compilation, out _);
        var generated = Generated(driver);
        await Assert.That(generated).Contains("TryAddScoped<global::Example.Handler>");
        await Assert.That(generated).DoesNotContain("AddHandlersFromThisAssembly");
        await Assert.That(generated).DoesNotContain("IRelayerBuilder");
    }

    [Test]
    [Arguments("IConsumerBuilder", "builder.Handler<OrderHandler>()")]
    [Arguments("ITransportConsumerBuilder", "builder.Consume<OrderHandler>(\"orders\")")]
    public async Task Handler_and_message_from_an_alias_only_reference_compile(string builderType, string registration)
    {
        var external = CreateCompilation("""
            using Relayer;
            using System.Threading;
            using System.Threading.Tasks;
            namespace Broker;
            public sealed class Order { }
            public sealed class OrderHandler : IHandler<Order>
            {
                public ValueTask HandleAsync(Order message, MessageContext context, CancellationToken cancellationToken) => default;
            }
            """).WithAssemblyName("Broker");
        using var stream = new MemoryStream();
        var emitted = external.Emit(stream);
        await Assert.That(emitted.Success).IsTrue();

        var compilation = CreateCompilation("""
            extern alias BrokerReference;
            using Relayer;
            using Order = BrokerReference::Broker.Order;
            using OrderHandler = BrokerReference::Broker.OrderHandler;

            public static class Registration
            {
                public static void Add(BUILDER builder) => REGISTRATION;
                public static void AddTyped(IConsumerBuilder builder) => builder.Handler<OrderHandler, Order>();
            }
            """.Replace("BUILDER", builderType).Replace("REGISTRATION", registration))
            .AddReferences(MetadataReference.CreateFromImage(stream.ToArray(), new MetadataReferenceProperties(aliases: ["BrokerReference"])));

        var driver = Run(compilation, out _);
        await Assert.That(Generated(driver)).Contains("TryAddScoped<@BrokerReference::Broker.OrderHandler>");
        await Assert.That(Generated(driver)).Contains("HandlerDescriptor<@BrokerReference::Broker.Order>");
    }

    [Test]
    public async Task Generated_types_keep_distinct_aliases_inside_nested_generic_arguments()
    {
        const string Contract = "namespace Shared; public sealed class Order { }";
        var orders = CreateCompilation(Contract).WithAssemblyName("Orders").ToMetadataReference(["Common", "Orders"]);
        var returns = CreateCompilation(Contract).WithAssemblyName("Returns").ToMetadataReference(["Common", "class"]);
        var compilation = CreateCompilation("""
            extern alias Orders;
            extern alias @class;
            using Relayer;
            using System.Threading;
            using System.Threading.Tasks;

            public class Envelope<T>
            {
                public sealed class Message<U> { }
            }

            public sealed class OrderHandler :
                IHandler<Envelope<Orders::Shared.Order>.Message<@class::Shared.Order>[]>
            {
                public ValueTask HandleAsync(Envelope<Orders::Shared.Order>.Message<@class::Shared.Order>[] message,
                    MessageContext context, CancellationToken cancellationToken) => default;
            }
            """).AddReferences(orders, returns);

        var driver = Run(compilation, out _);
        await Assert.That(Generated(driver))
            .Contains("HandlerDescriptor<global::Envelope<@Orders::Shared.Order>.Message<@class::Shared.Order>[]>");
    }

    [Test]
    public async Task Friend_assembly_registration_does_not_collide_with_the_referenced_assembly()
    {
        var library = CreateCompilation(Handlers).WithAssemblyName("Library").AddSyntaxTrees(CSharpSyntaxTree.ParseText("""
            using Relayer;
            [assembly: System.Runtime.CompilerServices.InternalsVisibleTo("GeneratorConsumer")]
            public static class LibraryRegistration
            {
                public static void Add(IConsumerBuilder builder) => builder.Handler<Example.Handler>();
            }
            """));
        Run(library, out var libraryOutput);
        using var stream = new MemoryStream();
        var emitted = libraryOutput.Emit(stream);
        await Assert.That(emitted.Success).IsTrue();
        var compilation = CreateCompilation("""
            using Relayer;
            using System.Threading;
            using System.Threading.Tasks;
            public sealed class Other : IHandler<long>
            {
                public ValueTask HandleAsync(long m, MessageContext c, CancellationToken ct) => default;
            }
            public static class Registration
            {
                // Separate statements: Roslyn 4.14's nullable walker crashes on the chained form here; the SDK compiler does not.
                public static void Add(IConsumerBuilder builder) { builder.Handler<Other>(); builder.Handler<Example.Handler>(); }
            }
            """).AddReferences(MetadataReference.CreateFromImage(stream.ToArray()));
        var driver = Run(compilation, out var output);
        await Assert.That(Generated(driver)).Contains("TryAddScoped<global::Other>");
        await Assert.That(Generated(driver)).Contains("TryAddScoped<global::Example.Handler>");
        var diagnostics = await output.WithAnalyzers([new ConfigurationAnalyzer()]).GetAnalyzerDiagnosticsAsync();
        await Assert.That(diagnostics.Where(static d => d.Id == "RLY003")).IsEmpty();
    }

    [Test]
    public async Task Tombstone_handler_uses_a_closed_native_dispatcher()
    {
        var driver = Run(CreateCompilation("""
            using Relayer;
            using System.Threading;
            using System.Threading.Tasks;
            public sealed class Order;
            public sealed class Handler : IHandler<Tombstone<Order>>
            {
                public ValueTask HandleAsync(Tombstone<Order> message, MessageContext context, CancellationToken token) => default;
            }
            """), out _);
        await Assert.That(Generated(driver)).Contains("TombstoneHandlerDescriptor<global::Order>");
        await Assert.That(Generated(driver)).Contains("IHandler<global::Relayer.Tombstone<global::Order>>");
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
    {
        return string.Join("\n", driver.GetRunResult().Results.Single().GeneratedSources
                    .Where(static s => s.HintName != "Microsoft.CodeAnalysis.EmbeddedAttribute.cs").Select(s => s.SourceText.ToString()));
    }
}
