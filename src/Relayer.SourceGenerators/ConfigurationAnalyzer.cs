using System.Collections.Concurrent;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Relayer.SourceGenerators;

/// <summary>Reports statically visible mistakes in message and consumer declarations.</summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class ConfigurationAnalyzer : DiagnosticAnalyzer
{
    private static readonly DiagnosticDescriptor MissingMessage = Rule("RLY001", "Message has no stable identity",
        "Message type '{0}' has no [Message] attribute. Add [Message(\"stable.name\")] to declare its wire identity.");
    private static readonly DiagnosticDescriptor DuplicateMessage = Rule("RLY002", "Duplicate message identity",
        "Message type '{0}' duplicates message identity '{1}' version {2}. Choose a unique name and version.", DiagnosticSeverity.Error);
    private static readonly DiagnosticDescriptor UnregisteredHandler = Rule("RLY003", "Handler is never registered",
        "Handler '{0}' is never registered in this compilation. Call Handler<{0}>() or AddHandlersFromThisAssembly().");
    private static readonly DiagnosticDescriptor EmptyConsumer = Rule("RLY004", "Consumer has no handler",
        "Consumer has no handler. Call consumer.Handler<THandler>() inside its configuration callback.");

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [MissingMessage, DuplicateMessage, UnregisteredHandler, EmptyConsumer];

    private static DiagnosticDescriptor Rule(string id, string title, string message,
        DiagnosticSeverity severity = DiagnosticSeverity.Warning) => new(id, title, message, "Relayer", severity,
        isEnabledByDefault: true, helpLinkUri: "https://relayer.tomlonghurst.dev/docs/reference/diagnostics#" + id.ToLowerInvariant());

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationStartAction(static start =>
        {
            var compilation = start.Compilation;
            var attribute = compilation.GetTypeByMetadataName("Relayer.MessageAttribute");
            if (attribute is null) return;

            var single = compilation.GetTypeByMetadataName("Relayer.IHandler`1");
            var batch = compilation.GetTypeByMetadataName("Relayer.IBatchHandler`1");
            var transport = compilation.GetTypeByMetadataName("Relayer.TransportBuilder`3");
            var consumer = compilation.GetTypeByMetadataName("Relayer.ConsumerBuilder`1");
            var consumerInterface = compilation.GetTypeByMetadataName("Relayer.IConsumerBuilder");
            var registration = compilation.GetTypeByMetadataName("Relayer.RelayerGeneratedHandlers");
            var handlers = new ConcurrentDictionary<INamedTypeSymbol, byte>(SymbolEqualityComparer.Default);
            var registered = new ConcurrentDictionary<ITypeSymbol, byte>(SymbolEqualityComparer.Default);
            var messages = new ConcurrentDictionary<INamedTypeSymbol, AttributeData>(SymbolEqualityComparer.Default);
            var missing = new ConcurrentDictionary<ITypeSymbol, ConcurrentDictionary<Location, byte>>(SymbolEqualityComparer.Default);
            var discovery = 0;

            void CheckMessage(ITypeSymbol message, Location use)
            {
                if (message.TypeKind is TypeKind.TypeParameter or TypeKind.Error) return;
                if (message is INamedTypeSymbol named) message = named.OriginalDefinition;
                if (message.GetAttributes().Any(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, attribute))) return;
                var location = message.Locations.FirstOrDefault(static l => l.IsInSource) ?? use;
                missing.GetOrAdd(message, static _ => new ConcurrentDictionary<Location, byte>()).TryAdd(location, 0);
            }

            start.RegisterSymbolAction(symbolContext =>
            {
                var type = (INamedTypeSymbol)symbolContext.Symbol;
                var identity = type.GetAttributes().FirstOrDefault(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, attribute));
                if (identity is not null) messages.TryAdd(type, identity);

                foreach (var contract in type.AllInterfaces)
                {
                    if (!SymbolEqualityComparer.Default.Equals(contract.OriginalDefinition, single)
                        && !SymbolEqualityComparer.Default.Equals(contract.OriginalDefinition, batch)) continue;
                    CheckMessage(contract.TypeArguments[0], type.Locations[0]);
                    if (type.TypeKind == TypeKind.Class && !type.IsAbstract && CanReference(type, compilation))
                        handlers.TryAdd(type, 0);
                }
            }, SymbolKind.NamedType);

            start.RegisterOperationAction(operationContext =>
            {
                var invocation = (IInvocationOperation)operationContext.Operation;
                var method = invocation.TargetMethod;
                if (SymbolEqualityComparer.Default.Equals(method.ContainingType.OriginalDefinition, transport))
                {
                    if (method.Name == "Send" && method.TypeArguments.Length == 1)
                        CheckMessage(method.TypeArguments[0], invocation.Syntax.GetLocation());
                    if (method.Name == "Consume") CheckConsumer(operationContext, invocation, consumer, consumerInterface);
                }

                // Only our generated registration methods count. Unrelated APIs with the same
                // names must not hide an unregistered handler.
                if (!SymbolEqualityComparer.Default.Equals(method.ContainingType, registration)) return;
                if (method.Name == "Handler" && method.TypeArguments.Length == 1)
                {
                    var handler = method.TypeArguments[0];
                    registered.TryAdd(handler, 0);
                    foreach (var contract in handler.AllInterfaces)
                        if (SymbolEqualityComparer.Default.Equals(contract.OriginalDefinition, single)
                            || SymbolEqualityComparer.Default.Equals(contract.OriginalDefinition, batch))
                            CheckMessage(contract.TypeArguments[0], invocation.Syntax.GetLocation());
                }
                else if (method.Name == "AddHandlersFromThisAssembly") Interlocked.Exchange(ref discovery, 1);
            }, OperationKind.Invocation);

            start.RegisterCompilationEndAction(end =>
            {
                foreach (var pair in missing)
                {
                    var declaration = pair.Key.DeclaringSyntaxReferences.FirstOrDefault();
                    var locations = declaration is null ? ImmutableArray<Location>.Empty
                        : ImmutableArray.Create(Location.Create(declaration.SyntaxTree, declaration.Span));
                    foreach (var location in pair.Value.Keys)
                        end.ReportDiagnostic(Diagnostic.Create(MissingMessage, location, locations, null, pair.Key.ToDisplayString()));
                }

                foreach (var group in messages.GroupBy(static pair => Identity(pair.Value)))
                {
                    if (group.Key.Name is null || group.Count() < 2) continue;
                    foreach (var pair in group)
                        end.ReportDiagnostic(Diagnostic.Create(DuplicateMessage, pair.Key.Locations[0],
                            pair.Key.ToDisplayString(), group.Key.Name, group.Key.Version));
                }

                if (discovery != 0) return;
                foreach (var handler in handlers.Keys)
                    if (!registered.ContainsKey(handler))
                        end.ReportDiagnostic(Diagnostic.Create(UnregisteredHandler, handler.Locations[0], handler.ToDisplayString()));
            });
        });
    }

    private static (string? Name, int Version) Identity(AttributeData attribute)
    {
        var name = attribute.ConstructorArguments.Length == 1 ? attribute.ConstructorArguments[0].Value as string : null;
        var version = 1;
        foreach (var argument in attribute.NamedArguments)
            if (argument.Key == "Version" && argument.Value.Value is int value) version = value;
        return (name, version);
    }

    private static bool CanReference(INamedTypeSymbol type, Compilation compilation) =>
        !type.IsFileLocal && !type.IsUnboundGenericType && type.TypeArguments.All(static t => t.TypeKind != TypeKind.TypeParameter)
        && compilation.IsSymbolAccessibleWithin(type, compilation.Assembly)
        && (type.ContainingType is null || CanReference(type.ContainingType, compilation));

    private static void CheckConsumer(OperationAnalysisContext context, IInvocationOperation invocation,
        INamedTypeSymbol? consumer, INamedTypeSymbol? consumerInterface)
    {
        var configure = invocation.Arguments.FirstOrDefault(static a => a.Parameter?.Name == "configure")?.Value;
        while (configure is IConversionOperation conversion) configure = conversion.Operand;
        if (configure is IDelegateCreationOperation creation) configure = creation.Target;
        if (configure is not IAnonymousFunctionOperation callback || callback.Symbol.Parameters.Length != 1) return;

        // A helper, alias or capture can register the handler. Warn only when
        // the inline callback is visibly empty apart from native options/portable settings.
        var parameter = callback.Symbol.Parameters[0];
        foreach (var operation in callback.Body.DescendantsAndSelf())
        {
            if (operation is IInvocationOperation call)
            {
                var method = call.TargetMethod;
                if (method.Name is not ("Concurrency" or "Batch")
                    || !SymbolEqualityComparer.Default.Equals(method.ContainingType.OriginalDefinition, consumer)
                        && !SymbolEqualityComparer.Default.Equals(method.ContainingType, consumerInterface)) return;
            }
            if (operation is IParameterReferenceOperation reference
                && SymbolEqualityComparer.Default.Equals(reference.Parameter, parameter)
                && reference.Parent is not IPropertyReferenceOperation { Property.Name: "Options" }
                && reference.Parent is not IInvocationOperation) return;
        }
        context.ReportDiagnostic(Diagnostic.Create(EmptyConsumer, callback.Syntax.GetLocation()));
    }
}
