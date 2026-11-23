using System.Text.Json;

namespace Relayer.AmazonSqs;

internal static class SnsFilterPolicy
{
    internal static bool AreEquivalent(JsonElement left, JsonElement right)
    {
        if (JsonElement.DeepEquals(left, right))
        {
            return true;
        }

        if (left.ValueKind != JsonValueKind.Object || right.ValueKind != JsonValueKind.Object ||
            left.GetPropertyCount() != right.GetPropertyCount())
        {
            return false;
        }

        // Stable ordering keeps duplicate property names in their original relative order, as DeepEquals does.
        var leftProperties = left.EnumerateObject().OrderBy(property => property.Name, StringComparer.Ordinal);
        var rightProperties = right.EnumerateObject().OrderBy(property => property.Name, StringComparer.Ordinal);

        return leftProperties.Zip(rightProperties).All(pair =>
            pair.First.Name == pair.Second.Name && FieldEquals(pair.First.Name, pair.First.Value, pair.Second.Value));
    }

    private static bool FieldEquals(string name, JsonElement left, JsonElement right)
    {
        if (left.ValueKind != JsonValueKind.Array || right.ValueKind != JsonValueKind.Array)
        {
            return AreEquivalent(left, right);
        }

        if (left.GetArrayLength() != right.GetArrayLength())
        {
            return false;
        }

        // A field's alternatives are unordered. Match operator operands inside an alternative stay in order.
        Func<JsonElement, JsonElement, bool> equals = name == "$or" && IsOrExpression(left) && IsOrExpression(right)
            ? AreEquivalent
            : JsonElement.DeepEquals;
        var unmatched = right.EnumerateArray().ToList();

        foreach (var alternative in left.EnumerateArray())
        {
            int index = unmatched.FindIndex(candidate => equals(alternative, candidate));

            if (index < 0)
            {
                return false;
            }

            unmatched.RemoveAt(index);
        }

        return true;
    }

    private static bool IsOrExpression(JsonElement array)
    {
        // SNS treats $or as a regular field when the array contains a match operator or fewer than two branches.
        return array.GetArrayLength() >= 2 && array.EnumerateArray().All(branch =>
            branch.ValueKind == JsonValueKind.Object && branch.EnumerateObject().All(property =>
                property.Name is not ("anything-but" or "cidr" or "equals-ignore-case" or "exists" or
                    "numeric" or "prefix" or "suffix" or "wildcard")));
    }
}
