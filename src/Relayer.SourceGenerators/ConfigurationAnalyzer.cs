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
        "Message type '{0}' has no stable identity. Add [Message(\"stable.name\")] or call MessageTypes.Add<T>(\"stable.name\") in this compilation.");
    private static readonly DiagnosticDescriptor DuplicateMessage = Rule("RLY002", "Duplicate message identity",
        "Message type '{0}' duplicates message identity '{1}' version {2}. Choose a unique name and version.", DiagnosticSeverity.Error);
    private static readonly DiagnosticDescriptor UnregisteredHandler = Rule("RLY003", "Handler is never registered",
        "Handler '{0}' is never registered in this compilation. Call consumer.Handler<{0}>() inside a consumer configuration callback.");
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
            var transport = compilation.GetTypeByMetadataName("Relayer.TransportBuilder`2");
            var consumer = compilation.GetTypeByMetadataName("Relayer.ConsumerBuilder`1");
            var consumerInterface = compilation.GetTypeByMetadataName("Relayer.IConsumerBuilder");
            var registration = compilation.GetTypeByMetadataName("Relayer.RelayerGeneratedHandlers");
            var marker = compilation.GetTypeByMetadataName("Relayer.IHandlerMarker");
            var messageRegistry = compilation.GetTypeByMetadataName("Relayer.Pipeline.MessageTypeRegistry");
            var helperRegistration = compilation.GetTypeByMetadataName("Relayer.RegistersMessageIdentityAttribute");
            var startupAttribute = compilation.GetTypeByMetadataName("Relayer.MessageIdentitiesRegisteredAtStartupAttribute");
            var tombstone = compilation.GetTypeByMetadataName("Relayer.Tombstone`1");
            var registeredAtStartup = compilation.Assembly.GetAttributes()
                .Where(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, startupAttribute))
                .Select(static a => a.ConstructorArguments.Length == 1 ? a.ConstructorArguments[0].Value as ITypeSymbol : null)
                .Where(static t => t is not null)
                .Select(static t => t is INamedTypeSymbol { IsUnboundGenericType: true } generic ? generic.OriginalDefinition : t!)
                .ToImmutableHashSet<ITypeSymbol>(SymbolEqualityComparer.Default);
            var explicitIdentities = new ConcurrentDictionary<ITypeSymbol, byte>(SymbolEqualityComparer.Default);
            var handlers = new ConcurrentDictionary<INamedTypeSymbol, byte>(SymbolEqualityComparer.Default);
            var registered = new ConcurrentDictionary<ITypeSymbol, byte>(SymbolEqualityComparer.Default);
            var messages = new ConcurrentDictionary<INamedTypeSymbol, AttributeData>(SymbolEqualityComparer.Default);
            var missing = new ConcurrentDictionary<ITypeSymbol, ConcurrentDictionary<Location, byte>>(SymbolEqualityComparer.Default);

            void CheckMessage(ITypeSymbol message, Location use)
            {
                if (message.TypeKind is TypeKind.TypeParameter or TypeKind.Error) return;
                if (message is INamedTypeSymbol wrapper && SymbolEqualityComparer.Default.Equals(wrapper.OriginalDefinition, tombstone))
                    message = wrapper.TypeArguments[0];
                if (registeredAtStartup.Contains(message)
                    || (message is INamedTypeSymbol named && registeredAtStartup.Contains(named.OriginalDefinition))) return;
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
                    // A public handler can be registered by a referencing assembly, so only
                    // handlers that this compilation alone can register are candidates.
                    if (type.TypeKind == TypeKind.Class && !type.IsAbstract && CanReference(type, compilation)
                        && !IsPublic(type))
                        handlers.TryAdd(type, 0);
                }
            }, SymbolKind.NamedType);

            start.RegisterOperationAction(operationContext =>
            {
                var invocation = (IInvocationOperation)operationContext.Operation;
                var method = invocation.TargetMethod;
                if (method.Name == "Add" && method.TypeArguments.Length == 1
                    && SymbolEqualityComparer.Default.Equals(method.ContainingType, messageRegistry))
                    explicitIdentities.TryAdd(method.TypeArguments[0], 0);
                if (helperRegistration is not null && method.TypeArguments.Length > 0)
                    AddHelperRegistrations(method, helperRegistration, explicitIdentities);

                if (SymbolEqualityComparer.Default.Equals(method.ContainingType.OriginalDefinition, transport))
                {
                    if (method.Name == "Send" && method.TypeArguments.Length == 1)
                        CheckMessage(method.TypeArguments[0], invocation.Syntax.GetLocation());
                    if (method.Name == "Consume") CheckConsumer(operationContext, invocation, consumer, consumerInterface);
                }

                if (method.TypeArguments.Length > 0)
                    foreach (var message in AddHandlerArguments(method, marker, single, batch, registered))
                        CheckMessage(message, invocation.Syntax.GetLocation());

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
            }, OperationKind.Invocation);

            start.RegisterCompilationEndAction(end =>
            {
                foreach (var pair in missing)
                {
                    if (explicitIdentities.ContainsKey(pair.Key)) continue;
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

                foreach (var handler in handlers.Keys)
                    if (!registered.ContainsKey(handler))
                        end.ReportDiagnostic(Diagnostic.Create(UnregisteredHandler, handler.Locations[0], handler.ToDisplayString()));
            });
        });
    }

    // A helper marked [RegistersMessageIdentity(nameof(T))] registers its type argument, even
    // when the helper lives in a referenced assembly where its Add<T> call is not visible.
    private static void AddHelperRegistrations(IMethodSymbol method, INamedTypeSymbol helperRegistration,
        ConcurrentDictionary<ITypeSymbol, byte> explicitIdentities)
    {
        foreach (var attribute in method.OriginalDefinition.GetAttributes())
        {
            if (!SymbolEqualityComparer.Default.Equals(attribute.AttributeClass, helperRegistration)
                || attribute.ConstructorArguments.Length != 1 || attribute.ConstructorArguments[0].Value is not string name) continue;
            for (var i = 0; i < method.TypeParameters.Length; i++)
                if (method.TypeParameters[i].Name == name) explicitIdentities.TryAdd(method.TypeArguments[i], 0);
        }
    }

    // A generic method whose type parameter must be a handler (Handler<THandler, TMessage>() or an
    // application helper that forwards to it) registers its type argument. Returns the message
    // types its IHandler<T>/IBatchHandler<T> constraints select, so their identities are checked.
    private static List<ITypeSymbol> AddHandlerArguments(IMethodSymbol method, INamedTypeSymbol? marker, INamedTypeSymbol? single,
        INamedTypeSymbol? batch, ConcurrentDictionary<ITypeSymbol, byte> registered)
    {
        var messages = new List<ITypeSymbol>();
        var parameters = method.OriginalDefinition.TypeParameters;
        for (var i = 0; i < parameters.Length; i++)
            foreach (var constraint in parameters[i].ConstraintTypes)
            {
                var contract = SymbolEqualityComparer.Default.Equals(constraint.OriginalDefinition, single)
                    || SymbolEqualityComparer.Default.Equals(constraint.OriginalDefinition, batch);
                if (!contract && !SymbolEqualityComparer.Default.Equals(constraint.OriginalDefinition, marker)) continue;
                registered.TryAdd(method.TypeArguments[i], 0);
                if (contract && constraint is INamedTypeSymbol { TypeArguments.Length: 1 } named)
                    messages.Add(named.TypeArguments[0] is ITypeParameterSymbol { TypeParameterKind: TypeParameterKind.Method } parameter
                        ? method.TypeArguments[parameter.Ordinal]
                        : named.TypeArguments[0]);
            }
        return messages;
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

    private static bool IsPublic(INamedTypeSymbol type) =>
        type.DeclaredAccessibility == Accessibility.Public && (type.ContainingType is null || IsPublic(type.ContainingType));

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
