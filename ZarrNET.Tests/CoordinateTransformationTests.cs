using ZarrNET.Core.OmeZarr.Coordinates;
using ZarrNET.Core.OmeZarr.Metadata;
using Xunit;

namespace ZarrNET.Tests;

public sealed class CoordinateTransformationTests
{
    [Fact]
    public void Affine_MapsAndInvertsPoint()
    {
        var transformation = new CoordinateTransformation
        {
            Type = "affine",
            Affine =
            [
                [2, 0, 10],
                [0, 3, -4]
            ]
        };

        var mapped = CoordinateTransformationEvaluator.TransformForward(transformation, [5, 2]);
        var inverted = CoordinateTransformationEvaluator.TransformInverse(transformation, mapped);

        Assert.Equal(new[] { 20.0, 2.0 }, mapped);
        Assert.Equal(new[] { 5.0, 2.0 }, inverted);
    }

    [Fact]
    public void SequenceAndAxisMapping_ComposeInDeclaredOrder()
    {
        var transformation = new CoordinateTransformation
        {
            Type = "sequence",
            Transformations =
            [
                new CoordinateTransformation { Type = "mapAxis", MapAxis = [1, 0] },
                new CoordinateTransformation { Type = "translation", Translation = [10, -2] }
            ]
        };

        var mapped = CoordinateTransformationEvaluator.TransformForward(transformation, [3, 7]);
        var inverted = CoordinateTransformationEvaluator.TransformInverse(transformation, mapped);

        Assert.Equal(new[] { 17.0, 1.0 }, mapped);
        Assert.Equal(new[] { 3.0, 7.0 }, inverted);
    }
}
