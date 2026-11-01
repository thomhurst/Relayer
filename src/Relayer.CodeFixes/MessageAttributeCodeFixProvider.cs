using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Formatting;

namespace Relayer.CodeFixes;

/// <summary>Adds an editable stable wire identity to a source message declaration.</summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(MessageAttributeCodeFixProvider)), Shared]
public sealed class MessageAttributeCodeFixProvider : CodeFixProvider
{
    public override ImmutableArray<string> FixableDiagnosticIds => ["RLY001"];

    public override FixAllProvider GetFixAllProvider() => WellKnownFixAllProviders.BatchFixer;

    public override async Task RegisterCodeFixesAsync(CodeFixContext context)
    {
        foreach (var diagnostic in context.Diagnostics)
        {
            if (diagnostic.AdditionalLocations.Count != 1) continue;
            var location = diagnostic.AdditionalLocations[0];
            var document = context.Document.Project.Solution.GetDocument(location.SourceTree);
            if (document is null) continue;
            var root = await document.GetSyntaxRootAsync(context.CancellationToken).ConfigureAwait(false);
            if (root?.FindNode(location.SourceSpan) is not TypeDeclarationSyntax declaration
                || declaration is InterfaceDeclarationSyntax) continue;
            context.RegisterCodeFix(CodeAction.Create("Add [Message] attribute (choose a stable wire name)",
                ct => AddAttributeAsync(document, location, ct), "Relayer.AddMessageAttribute"), diagnostic);
        }
    }

    private static async Task<Solution> AddAttributeAsync(Document document, Location location, CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken).ConfigureAwait(false);
        var declaration = root?.FindNode(location.SourceSpan) as TypeDeclarationSyntax;
        if (root is null || declaration is null) return document.Project.Solution;
        // This is an editable suggestion, never a runtime CLR-name fallback. The user
        // must choose the contract's permanent wire name before publishing it.
        var attribute = SyntaxFactory.Attribute(SyntaxFactory.ParseName("global::Relayer.MessageAttribute"),
            SyntaxFactory.AttributeArgumentList(SyntaxFactory.SingletonSeparatedList(
                SyntaxFactory.AttributeArgument(SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression,
                    SyntaxFactory.Literal(declaration.Identifier.ValueText))))));
        var updated = declaration.WithoutLeadingTrivia().AddAttributeLists(
                SyntaxFactory.AttributeList(SyntaxFactory.SingletonSeparatedList(attribute)))
            .WithLeadingTrivia(declaration.GetLeadingTrivia()).WithAdditionalAnnotations(Formatter.Annotation);
        return document.WithSyntaxRoot(root.ReplaceNode(declaration, updated)).Project.Solution;
    }
}
