namespace Relayer.Tests;

public class MessageFeaturesTests
{
    [Test]
    public async Task Get_returns_null_when_feature_is_missing()
    {
        var features = new MessageFeatures();

        await Assert.That(features.Get<FeatureA>()).IsNull();
    }

    [Test]
    public async Task Set_then_Get_returns_the_same_instance()
    {
        var features = new MessageFeatures();
        var feature = new FeatureA();

        features.Set(feature);

        await Assert.That(features.Get<FeatureA>()).IsSameReferenceAs(feature);
    }

    [Test]
    public async Task Set_replaces_an_existing_feature_of_the_same_type()
    {
        var features = new MessageFeatures();
        var replacement = new FeatureA();

        features.Set(new FeatureA());
        features.Set(replacement);

        await Assert.That(features.Get<FeatureA>()).IsSameReferenceAs(replacement);
    }

    [Test]
    public async Task Grows_beyond_initial_capacity()
    {
        var features = new MessageFeatures();
        var b = new FeatureB();

        features.Set(new FeatureA());
        features.Set(b);
        features.Set(new FeatureC());
        features.Set(new FeatureD());
        features.Set(new FeatureE());

        await Assert.That(features.Get<FeatureB>()).IsSameReferenceAs(b);
        await Assert.That(features.Get<FeatureE>()).IsNotNull();
    }

    [Test]
    public async Task GetRequired_throws_a_descriptive_error_when_missing()
    {
        var features = new MessageFeatures();

        await Assert.That(() => features.GetRequired<FeatureA>())
            .Throws<InvalidOperationException>()
            .WithMessageContaining(nameof(FeatureA));
    }

    [Test]
    public async Task Clear_removes_all_features()
    {
        var features = new MessageFeatures();
        features.Set(new FeatureA());

        features.Clear();

        await Assert.That(features.Get<FeatureA>()).IsNull();
    }

    private sealed class FeatureA;
    private sealed class FeatureB;
    private sealed class FeatureC;
    private sealed class FeatureD;
    private sealed class FeatureE;
}
