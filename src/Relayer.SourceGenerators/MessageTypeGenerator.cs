using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Relayer.SourceGenerators;

/// <summary>
/// Finds types marked with <c>[Relayer.Message]</c> and emits an explicit, reflection-free
/// <c>AddMessageTypesFromThisAssembly()</c> extension and JSON context (docs/decisions.md D4, D7, D8).
/// </summary>
[Generator(LanguageNames.CSharp)]
public sealed class MessageTypeGenerator : IIncrementalGenerator
{
    private const string MessageAttributeName = "Relayer.MessageAttribute";

    private static readonly DiagnosticDescriptor InaccessibleMessage = new(
        "RLYM002", "Message type is inaccessible",
        "Message type '{0}' cannot be accessed by generated registration and JSON metadata. Make the message and its containing types assembly-accessible and non-file-local.",
        "Relayer", DiagnosticSeverity.Error, isEnabledByDefault: true,
        helpLinkUri: "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlym002");

    private static readonly DiagnosticDescriptor OpenGenericMessage = new(
        "RLYM001", "Message declaration has unbound type parameters",
        "Message '{0}' has type parameters on itself or a containing type. Declare a concrete, non-generic message outside generic containing types.",
        "Relayer", DiagnosticSeverity.Error, isEnabledByDefault: true,
        helpLinkUri: "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlym001");

    private static readonly DiagnosticDescriptor RefLikeMessage = new(
        "RLYM003", "Message type is ref-like",
        "Message type '{0}' is ref-like and cannot be registered or serialized. Use an ordinary struct, class or record for the wire message.",
        "Relayer", DiagnosticSeverity.Error, isEnabledByDefault: true,
        helpLinkUri: "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlym003");

    private static readonly DiagnosticDescriptor StaticMessage = new(
        "RLYM004", "Message type is static",
        "Message type '{0}' is static and cannot be used by generated registration or JSON metadata. Declare a non-static message class, struct or record.",
        "Relayer", DiagnosticSeverity.Error, isEnabledByDefault: true,
        helpLinkUri: "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlym004");

    private static readonly DiagnosticDescriptor NullMessageName = new(
        "RLYM005", "Message name cannot be null",
        "Message '{0}' has a null name. A stable message name cannot be null; specify a permanent wire name in [Message(\"name\")].",
        "Relayer", DiagnosticSeverity.Error, isEnabledByDefault: true,
        helpLinkUri: "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlym005");

    private static readonly DiagnosticDescriptor InvalidMessageName = new(
        "RLYM006", "Message name is empty or whitespace",
        "Message '{0}' must have a name containing at least one non-whitespace character",
        "Relayer", DiagnosticSeverity.Error, isEnabledByDefault: true,
        helpLinkUri: "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlym006");

    private static readonly DiagnosticDescriptor InvalidMessageVersion = new(
        "RLYM007", "Message version is below 1",
        "Message '{0}' must have a version of at least 1",
        "Relayer", DiagnosticSeverity.Error, isEnabledByDefault: true,
        helpLinkUri: "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#rlym007");

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var refLikeMessages = context.SyntaxProvider.ForAttributeWithMetadataName(
                MessageAttributeName,
                predicate: static (_, _) => true,
                transform: static (ctx, _) => ctx.TargetSymbol is INamedTypeSymbol { IsRefLikeType: true } type
                    && ctx.TargetNode is TypeDeclarationSyntax declaration
                        ? new RefLikeMessageModel(type.ToDisplayString(), declaration.SyntaxTree.FilePath,
                            declaration.Identifier.Span, declaration.Identifier.GetLocation().GetLineSpan().Span)
                        : null)
            .Where(static model => model is not null);
        context.RegisterSourceOutput(refLikeMessages, static (spc, model) =>
            spc.ReportDiagnostic(Diagnostic.Create(RefLikeMessage,
                Location.Create(model!.Path, model.Span, model.LineSpan), model.Name)));

        var declarations = context.SyntaxProvider
            .ForAttributeWithMetadataName(
                MessageAttributeName,
                predicate: static (_, _) => true,
                transform: static (ctx, _) => ToModel(ctx))
            .Where(static m => m is not null)
            .Select(static (m, _) => m!)
            .WithTrackingName("MessageDeclarations");

        context.RegisterSourceOutput(declarations, static (spc, model) =>
        {
            if (model.NullNameLocation is { } nullNameLocation)
            {
                spc.ReportDiagnostic(Diagnostic.Create(NullMessageName,
                    Location.Create(nullNameLocation.FilePath, nullNameLocation.Span, nullNameLocation.LineSpan), model.FullyQualifiedName));
            }
            if (model.InvalidIdentityLocation is { } identityLocation)
            {
                var attributeLocation = Location.Create(identityLocation.FilePath, identityLocation.Span, identityLocation.LineSpan);
                if (string.IsNullOrWhiteSpace(model.Name))
                    spc.ReportDiagnostic(Diagnostic.Create(InvalidMessageName, attributeLocation, model.FullyQualifiedName));
                if (model.Version < 1)
                    spc.ReportDiagnostic(Diagnostic.Create(InvalidMessageVersion, attributeLocation, model.FullyQualifiedName));
            }
            if (model.InaccessibleLocation is { } inaccessibleLocation)
            {
                spc.ReportDiagnostic(Diagnostic.Create(model.IsStatic ? StaticMessage : InaccessibleMessage,
                    Location.Create(inaccessibleLocation.FilePath, inaccessibleLocation.Span, inaccessibleLocation.LineSpan), model.FullyQualifiedName));
            }
            if (model.OpenGenericLocation is { } location)
            {
                spc.ReportDiagnostic(Diagnostic.Create(OpenGenericMessage,
                    Location.Create(location.FilePath, location.Span, location.LineSpan), model.FullyQualifiedName));
            }
        });
        var messages = declarations.Where(static m => m.OpenGenericLocation is null && m.InaccessibleLocation is null && m.NullNameLocation is null && m.InvalidIdentityLocation is null).Collect();

        context.RegisterSourceOutput(messages, static (spc, models) => Emit(spc, models));
        context.RegisterSourceOutput(messages.Combine(context.CompilationProvider),
            static (spc, input) => EmitJsonContext(spc, input.Left, input.Right));
    }

    private static MessageModel? ToModel(GeneratorAttributeSyntaxContext context)
    {
        if (context.TargetSymbol is not INamedTypeSymbol type || type.IsRefLikeType || context.Attributes.Length == 0)
        {
            return null;
        }

        if (type.IsStatic || !CanReference(type, context.SemanticModel.Compilation))
        {
            var location = context.TargetNode.GetLocation();
            return new MessageModel(type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), "", 1,
                InaccessibleLocation: new DiagnosticLocation(location.GetLineSpan().Path, location.SourceSpan, location.GetLineSpan().Span), IsStatic: type.IsStatic);
        }

        var attribute = context.Attributes[0];
        if (attribute.ConstructorArguments.Length == 1 && attribute.ConstructorArguments[0].IsNull)
        {
            var location = attribute.ApplicationSyntaxReference?.GetSyntax().GetLocation() ?? context.TargetNode.GetLocation();
            return new MessageModel(type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat), "", 1,
                NullNameLocation: new DiagnosticLocation(location.GetLineSpan().Path, location.SourceSpan, location.GetLineSpan().Span));
        }
        if (attribute.ConstructorArguments.Length != 1 || attribute.ConstructorArguments[0].Value is not string name)
        {
            return null;
        }

        var version = 1;
        foreach (var argument in attribute.NamedArguments)
        {
            if (argument.Key == "Version" && argument.Value.Value is int v)
            {
                version = v;
            }
        }

        var fullyQualifiedName = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        DiagnosticLocation? openGenericLocation = null;
        for (var current = type; current is not null; current = current.ContainingType)
        {
            if (current.Arity != 0)
            {
                var location = context.TargetNode.GetLocation();
                openGenericLocation = new DiagnosticLocation(location.GetLineSpan().Path,
                    location.SourceSpan, location.GetLineSpan().Span);
                break;
            }
        }
        DiagnosticLocation? invalidIdentityLocation = null;
        if (string.IsNullOrWhiteSpace(name) || version < 1)
        {
            var location = attribute.ApplicationSyntaxReference!.GetSyntax().GetLocation();
            invalidIdentityLocation = new DiagnosticLocation(location.GetLineSpan().Path,
                location.SourceSpan, location.GetLineSpan().Span);
        }
        return new MessageModel(fullyQualifiedName, name, version, openGenericLocation,
            InvalidIdentityLocation: invalidIdentityLocation);
    }

    private static void Emit(SourceProductionContext context, ImmutableArray<MessageModel> models)
    {
        if (models.IsDefaultOrEmpty)
        {
            return;
        }

        var builder = new StringBuilder();
        builder.AppendLine("// <auto-generated/>");
        builder.AppendLine("#nullable enable");
        builder.AppendLine();
        builder.AppendLine("namespace Relayer");
        builder.AppendLine("{");
        builder.AppendLine("    /// <summary>Message types declared with [Message] in this assembly.</summary>");
        builder.AppendLine("    [global::System.CodeDom.Compiler.GeneratedCode(\"Relayer.SourceGenerators\", \"1.0.0\")]");
        builder.AppendLine("    internal static class RelayerGeneratedMessageTypes");
        builder.AppendLine("    {");
        builder.AppendLine("        /// <summary>Registers every [Message] type declared in this assembly.</summary>");
        builder.AppendLine("        public static global::Relayer.IRelayerBuilder AddMessageTypesFromThisAssembly(this global::Relayer.IRelayerBuilder builder, global::System.Text.Json.JsonSerializerOptions? serializerOptions = null)");
        builder.AppendLine("        {");
        builder.AppendLine("            global::System.ArgumentNullException.ThrowIfNull(builder);");

        foreach (var model in models.OrderBy(m => m.Name, StringComparer.Ordinal).ThenBy(m => m.Version))
        {
            builder.Append("            builder.MessageTypes.Add<")
                .Append(model.FullyQualifiedName)
                .Append(">(")
                .Append(SymbolDisplay.FormatLiteral(model.Name, quote: true))
                .Append(", ")
                .Append(model.Version)
                .AppendLine(");");
        }

        builder.AppendLine("            return global::Relayer.JsonSerializerBuilderExtensions.UseJsonSerializer(builder, serializerOptions is null ? new RelayerGeneratedJsonContext() : new RelayerGeneratedJsonContext(new global::System.Text.Json.JsonSerializerOptions(serializerOptions)));");
        builder.AppendLine("        }");
        builder.AppendLine("    }");
        builder.AppendLine("}");

        context.AddSource("RelayerGeneratedMessageTypes.g.cs", SourceText.From(builder.ToString(), Encoding.UTF8));
    }

    private static void EmitJsonContext(SourceProductionContext context, ImmutableArray<MessageModel> models, Compilation compilation)
    {
        if (models.IsDefaultOrEmpty) return;

        var source = new StringBuilder("// <auto-generated/>\n#nullable enable\nnamespace Relayer\n{\n");
        var index = 0;
        foreach (var model in models.OrderBy(m => m.FullyQualifiedName, StringComparer.Ordinal))
        {
            source.Append("    [global::System.Text.Json.Serialization.JsonSerializable(typeof(")
                .Append(model.FullyQualifiedName).Append("), TypeInfoPropertyName = \"RelayerMessage")
                .Append(index++).AppendLine("\")]");
        }
        source.AppendLine("    internal sealed partial class RelayerGeneratedJsonContext : global::System.Text.Json.Serialization.JsonSerializerContext { }");
        source.AppendLine("}");

        var text = SourceText.From(source.ToString(), Encoding.UTF8);
        var parseOptions = (CSharpParseOptions)compilation.SyntaxTrees.First().Options;
        var declaration = CSharpSyntaxTree.ParseText(text, parseOptions, cancellationToken: context.CancellationToken);
        context.AddSource("RelayerGeneratedJsonContext.Declaration.g.cs", text);

        // Roslyn generators cannot see one another's output. Run STJ on the augmented
        // compilation and forward only our context, leaving user contexts to the SDK.
        var result = CSharpGeneratorDriver.Create([JsonGeneratorLoader.Create().AsSourceGenerator()], parseOptions: parseOptions)
            .RunGenerators(compilation.AddSyntaxTrees(declaration), context.CancellationToken)
            .GetRunResult();
        foreach (var generated in result.Results.SelectMany(r => r.GeneratedSources))
        {
            if (generated.HintName.StartsWith("RelayerGeneratedJsonContext.", StringComparison.Ordinal))
            {
                context.AddSource(generated.HintName, generated.SourceText);
            }
        }
        foreach (var diagnostic in result.Diagnostics)
        {
            if (diagnostic.Location.SourceTree != declaration)
            {
                context.ReportDiagnostic(diagnostic);
                continue;
            }
            // The temporary declaration is not part of Roslyn's input compilation, so
            // diagnostics referring to it must be forwarded without that source location.
            var descriptor = new DiagnosticDescriptor(diagnostic.Id, diagnostic.Descriptor.Title, "{0}",
                diagnostic.Descriptor.Category, diagnostic.Severity, isEnabledByDefault: true,
                helpLinkUri: diagnostic.Descriptor.HelpLinkUri);
            context.ReportDiagnostic(Diagnostic.Create(descriptor, Location.None, diagnostic.GetMessage()));
        }
    }

    private static bool CanReference(INamedTypeSymbol type, Compilation compilation) =>
        !type.IsFileLocal
        && compilation.IsSymbolAccessibleWithin(type, compilation.Assembly)
        && (type.ContainingType is null || CanReference(type.ContainingType, compilation));

    private sealed record MessageModel(string FullyQualifiedName, string Name, int Version, DiagnosticLocation? OpenGenericLocation = null, DiagnosticLocation? InaccessibleLocation = null, bool IsStatic = false, DiagnosticLocation? NullNameLocation = null, DiagnosticLocation? InvalidIdentityLocation = null);

    private sealed record DiagnosticLocation(string FilePath, TextSpan Span, LinePositionSpan LineSpan);

    private sealed record RefLikeMessageModel(string Name, string Path, TextSpan Span, LinePositionSpan LineSpan);
}
