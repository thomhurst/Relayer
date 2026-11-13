using Relayer.Transports;
using System.Reflection;
using System.Runtime.Loader;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Relayer.SourceGenerators;

namespace Relayer.SourceGenerators.Tests;

public class MessageTypeGeneratorTests
{
    [Test]
    [Arguments("null", false)]
    [Arguments("null!", false)]
    [Arguments("null", true)]
    [Arguments("null!", true)]
    public async Task Null_message_names_report_diagnostic_and_preserve_valid_output(string name, bool includeValid)
    {
        var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator).Select(static p => MetadataReference.CreateFromFile(p)).ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(MessageAttribute).Assembly.Location));
        var source = $"#nullable enable\n[Relayer.Message({name})] public sealed record InvalidOrder(int Id);"
            + (includeValid ? "\n[Relayer.Message(\"orders.valid\")] public sealed record ValidOrder(int Id);" : "");
        var tree = CSharpSyntaxTree.ParseText(source, path: "Messages.cs");
        var compilation = CSharpCompilation.Create("NullMessageConsumer", [tree], references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        GeneratorDriver driver = CSharpGeneratorDriver.Create(new MessageTypeGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);

        await Assert.That(diagnostics.Length).IsEqualTo(1);
        var diagnostic = diagnostics[0];
        await Assert.That(diagnostic.Id).IsEqualTo("RLYM005");
        await Assert.That(diagnostic.Severity).IsEqualTo(DiagnosticSeverity.Error);
        await Assert.That(diagnostic.GetMessage()).Contains("cannot be null");
        await Assert.That(diagnostic.Location.GetLineSpan().Path).IsEqualTo("Messages.cs");
        await Assert.That(tree.GetText().ToString(diagnostic.Location.SourceSpan)).IsEqualTo($"Relayer.Message({name})");
        await Assert.That(diagnostic.Descriptor.HelpLinkUri)
            .IsEqualTo("https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlym005");
        await Assert.That(string.Join("\n", output.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error)))
            .IsEqualTo("");
        var generated = string.Join("\n", driver.GetRunResult().GeneratedTrees.Select(t => t.ToString()));
        await Assert.That(generated).DoesNotContain("InvalidOrder");
        if (includeValid)
        {
            await Assert.That(generated).Contains("Add<global::ValidOrder>(\"orders.valid\", 1)");
            await Assert.That(generated).Contains("typeof(global::ValidOrder)");
        }
        else
        {
            await Assert.That(generated).IsEqualTo("");
        }
    }

    [Test]
    [Arguments("\"\"", "RLYM006")]
    [Arguments("\"   \"", "RLYM006")]
    [Arguments("\"\\t\\r\\n\\u2003\"", "RLYM006")]
    [Arguments("\"orders.placed\", Version = 0", "RLYM007")]
    [Arguments("\"orders.placed\", Version = -1", "RLYM007")]
    public async Task Invalid_identity_constants_are_diagnosed_and_excluded(string arguments, string diagnosticId)
    {
        foreach (var includeValid in new[] { false, true })
        {
            var source = $"[Relayer.Message({arguments})] public sealed record Invalid(int Id);";
            if (includeValid)
                source += "[Relayer.Message(\" valid \")] public sealed record Valid(int Id);";
            var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
                .Split(Path.PathSeparator).Select(static p => MetadataReference.CreateFromFile(p)).ToList();
            references.Add(MetadataReference.CreateFromFile(typeof(MessageAttribute).Assembly.Location));
            var tree = CSharpSyntaxTree.ParseText(source, path: "Message.cs");
            var compilation = CSharpCompilation.Create("InvalidIdentityConsumer", [tree], references,
                new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
            GeneratorDriver driver = CSharpGeneratorDriver.Create(new MessageTypeGenerator());
            driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);

            await Assert.That(diagnostics.Length).IsEqualTo(1);
            var diagnostic = diagnostics[0];
            await Assert.That(diagnostic.Id).IsEqualTo(diagnosticId);
            await Assert.That(diagnostic.Severity).IsEqualTo(DiagnosticSeverity.Error);
            await Assert.That(tree.GetText().ToString(diagnostic.Location.SourceSpan))
                .IsEqualTo($"Relayer.Message({arguments})");
            await Assert.That(diagnostic.Descriptor.HelpLinkUri)
                .IsEqualTo($"https://relayer.tomlonghurst.dev/docs/reference/diagnostics#{diagnosticId.ToLowerInvariant()}");
            var generated = string.Join("\n", driver.GetRunResult().GeneratedTrees.Select(t => t.ToString()));
            await Assert.That(generated).DoesNotContain("global::Invalid");
            if (includeValid)
            {
                await Assert.That(generated).Contains("Add<global::Valid>(\" valid \", 1)");
                await Assert.That(generated).Contains("typeof(global::Valid)");
            }
            else
                await Assert.That(driver.GetRunResult().GeneratedTrees.Length).IsEqualTo(0);
            await Assert.That(string.Join("\n", output.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error)))
                .IsEqualTo("");
        }
    }

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
    [Arguments("public ref struct StackMessage { public int Value { get; set; } }")]
    [Arguments("public readonly ref struct StackMessage { public int Value { get; } }")]
    public async Task Ref_like_messages_are_diagnosed_and_excluded_without_breaking_valid_messages(string declaration)
    {
        var source = """
            [Relayer.Message("stack")]
            DECLARATION
            [Relayer.Message("ordinary")]
            public struct OrdinaryMessage { public int Value { get; set; } }
            [Relayer.Message("record")]
            public sealed record RecordMessage(int Value);
            """.Replace("DECLARATION", declaration);
        var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator).Select(static p => MetadataReference.CreateFromFile(p)).ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(MessageAttribute).Assembly.Location));
        var tree = CSharpSyntaxTree.ParseText(source, path: "Messages.cs");
        var compilation = CSharpCompilation.Create("MessageConsumer", [tree], references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        GeneratorDriver driver = CSharpGeneratorDriver.Create(new MessageTypeGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);

        var diagnostic = diagnostics.Single();
        await Assert.That(diagnostic.Id).IsEqualTo("RLYM003");
        await Assert.That(diagnostic.Severity).IsEqualTo(DiagnosticSeverity.Error);
        await Assert.That(diagnostic.Location.GetLineSpan().Path).IsEqualTo("Messages.cs");
        await Assert.That(source.Substring(diagnostic.Location.SourceSpan.Start, diagnostic.Location.SourceSpan.Length))
            .IsEqualTo("StackMessage");
        await Assert.That(diagnostic.GetMessage()).Contains("ordinary struct, class or record");
        await Assert.That(diagnostic.Descriptor.HelpLinkUri)
            .IsEqualTo("https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlym003");
        var generated = string.Join("\n", driver.GetRunResult().GeneratedTrees.Select(t => t.ToString()));
        await Assert.That(generated).DoesNotContain("StackMessage");
        await Assert.That(generated).Contains("MessageTypes.Add<global::OrdinaryMessage>");
        await Assert.That(generated).Contains("MessageTypes.Add<global::RecordMessage>");
        await Assert.That(generated).Contains("JsonTypeInfo<global::OrdinaryMessage>");
        await Assert.That(generated).Contains("JsonTypeInfo<global::RecordMessage>");
        await Assert.That(string.Join("\n", output.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error)))
            .IsEqualTo("");
        using var image = new MemoryStream();
        await Assert.That(output.Emit(image).Success).IsTrue();
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
            using Relayer.Transports;
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
                        if (!builder.Infrastructure().MessageTypes.TryGetMessageType(typeof(Order), out identity))
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

    [Test]
    [Arguments("null")]
    [Arguments("\"\"")]
    [Arguments("\"class\"")]
    [Arguments("\"Add-Orders\"")]
    [Arguments("\"AddMessageTypesFromThisAssembly\"")]
    public async Task Invalid_export_names_are_diagnosed(string name)
    {
        var compilation = CreateCompilation("InvalidExport", $"[assembly: Relayer.GenerateMessageTypes({name})]");
        GeneratorDriver driver = CSharpGeneratorDriver.Create(new MessageTypeGenerator());
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);
        await Assert.That(diagnostics.Single().Id).IsEqualTo("RLYM008");
        await Assert.That(string.Join("\n", output.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error))).IsEqualTo("");
    }

    [Test]
    public async Task Empty_contracts_export_a_callable_no_op()
    {
        var output = Generate(CreateCompilation("EmptyExport", "[assembly: Relayer.GenerateMessageTypes(\"AddEmptyMessages\") ]"));
        using var image = new MemoryStream();
        var result = output.Emit(image);
        await Assert.That(string.Join("\n", result.Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error))).IsEqualTo("");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Referenced_contracts_compose_with_host_messages_and_validate_duplicate_identities(bool duplicate)
    {
        var orders = Generate(CreateCompilation("Orders_" + Guid.NewGuid().ToString("N"), """
            [assembly: Relayer.GenerateMessageTypes("AddOrderMessages")]
            namespace Orders;
            [Relayer.Message("orders.placed", Version = 2)] public sealed record Order(int OrderId);
            """));
        var billing = Generate(CreateCompilation("Billing_" + Guid.NewGuid().ToString("N"), $$"""
            [assembly: Relayer.GenerateMessageTypes("AddBillingMessages")]
            namespace Billing;
            [Relayer.Message("{{(duplicate ? "orders.placed" : "billing.paid")}}", Version = 2)]
            public sealed record Payment(int PaymentId);
            """));
        using var ordersImage = new MemoryStream();
        using var billingImage = new MemoryStream();
        await Assert.That(orders.Emit(ordersImage).Success).IsTrue();
        await Assert.That(billing.Emit(billingImage).Success).IsTrue();
        var host = Generate(CreateCompilation("Host_" + Guid.NewGuid().ToString("N"), """
            using System;
            using System.Buffers;
            using System.Text;
            using System.Text.Json;
            using Microsoft.Extensions.DependencyInjection;
            using Relayer;
            using Relayer.Pipeline;
            using Relayer.Serialization;
            [Message("host.local")] public sealed record Local(int LocalId);
            public static class Probe
            {
                public static string Run()
                {
                    var services = new ServiceCollection();
                    try
                    {
                        services.AddRelayer(builder =>
                        {
                            builder.AddOrderMessages(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
                            builder.AddBillingMessages();
                            builder.AddMessageTypesFromThisAssembly();
                        });
                    }
                    catch (InvalidOperationException e) { return e.Message; }
                    using var provider = services.BuildServiceProvider();
                    var serializer = provider.GetRequiredService<IMessageSerializer>();
                    var registry = provider.GetRequiredService<MessageTypeRegistry>();
                    if (!registry.TryGetMessageType(typeof(Orders.Order), out var identity)
                        || identity != new MessageType("orders.placed", 2)) throw new Exception("Missing identity");
                    var buffer = new ArrayBufferWriter<byte>();
                    serializer.Serialize(new Orders.Order(42), buffer);
                    var json = Encoding.UTF8.GetString(buffer.WrittenSpan);
                    if (serializer.Deserialize<Orders.Order>(new ReadOnlySequence<byte>(buffer.WrittenMemory)).OrderId != 42)
                        throw new Exception("Roundtrip failed");
                    buffer.Clear();
                    serializer.Serialize(new Billing.Payment(7), buffer);
                    if (serializer.Deserialize<Billing.Payment>(new ReadOnlySequence<byte>(buffer.WrittenMemory)).PaymentId != 7)
                        throw new Exception("Billing context lost");
                    buffer.Clear();
                    serializer.Serialize(new Local(9), buffer);
                    if (serializer.Deserialize<Local>(new ReadOnlySequence<byte>(buffer.WrittenMemory)).LocalId != 9)
                        throw new Exception("Host context lost");
                    return json;
                }
            }
            """).AddReferences(MetadataReference.CreateFromImage(ordersImage.ToArray()), MetadataReference.CreateFromImage(billingImage.ToArray())));
        using var hostImage = new MemoryStream();
        var emitted = host.Emit(hostImage);
        await Assert.That(string.Join("\n", emitted.Diagnostics.Where(d => d.Severity >= DiagnosticSeverity.Warning))).IsEqualTo("");
        var loader = new AssemblyLoadContext(host.AssemblyName, isCollectible: true);
        try
        {
            ordersImage.Position = billingImage.Position = hostImage.Position = 0;
            loader.LoadFromStream(ordersImage);
            loader.LoadFromStream(billingImage);
            var assembly = loader.LoadFromStream(hostImage);
            var result = (string)assembly.GetType("Probe")!.GetMethod("Run")!.Invoke(null, null)!;
            await Assert.That(result).IsEqualTo(duplicate
                ? "'orders.placed;v=2' is already registered to Orders.Order."
                : "{\"orderId\":42}");
        }
        finally { loader.Unload(); }
    }

    private static CSharpCompilation CreateCompilation(string name, string source)
    {
        var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator).Select(static p => MetadataReference.CreateFromFile(p)).ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(MessageAttribute).Assembly.Location));
        return CSharpCompilation.Create(name, [CSharpSyntaxTree.ParseText(source)], references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
    }

    private static Compilation Generate(CSharpCompilation compilation)
    {
        GeneratorDriver driver = CSharpGeneratorDriver.Create(new MessageTypeGenerator());
        driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);
        var errors = diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error);
        if (errors.Any()) throw new InvalidOperationException(string.Join("\n", errors));
        return output;
    }

    private sealed class CompetingGeneratorLoadContext(byte[] competingImage) : AssemblyLoadContext
    {
        protected override Assembly? Load(AssemblyName assemblyName) =>
            assemblyName.Name == "System.Text.Json.SourceGeneration"
                ? LoadFromStream(new MemoryStream(competingImage))
                : null;
    }
}
