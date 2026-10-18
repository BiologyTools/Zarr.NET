using ZarrNET.Core.OmeZarr.Metadata;

namespace ZarrNET.Core.OmeZarr.Coordinates;

/// <summary>
/// Finds and evaluates point-transform paths through OME-Zarr 0.6 scene
/// metadata. Each edge is traversed forward or, when possible, in reverse.
/// </summary>
public sealed class CoordinateTransformationGraph
{
    private readonly CoordinateTransformation[] _transformations;

    public CoordinateTransformationGraph(IEnumerable<CoordinateTransformation> transformations)
    {
        _transformations = transformations?.ToArray()
            ?? throw new ArgumentNullException(nameof(transformations));
    }

    public static CoordinateTransformationGraph FromScene(SceneMetadata scene)
    {
        ArgumentNullException.ThrowIfNull(scene);
        return new CoordinateTransformationGraph(scene.CoordinateTransformations);
    }

    /// <summary>Returns the ordered edges between two named coordinate systems.</summary>
    public IReadOnlyList<CoordinateTransformEdge> FindPath(
        CoordinateSystemReference source,
        CoordinateSystemReference destination)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(destination);
        var sourceKey = CoordinateSystemKey.From(source);
        var destinationKey = CoordinateSystemKey.From(destination);
        var queue = new Queue<(CoordinateSystemKey Node, List<CoordinateTransformEdge> Path)>();
        var visited = new HashSet<CoordinateSystemKey> { sourceKey };
        queue.Enqueue((sourceKey, new List<CoordinateTransformEdge>()));

        while (queue.Count > 0)
        {
            var (node, path) = queue.Dequeue();
            if (node == destinationKey) return path;
            foreach (var edge in EdgesFrom(node))
            {
                var edgeDestination = new CoordinateSystemKey(edge.DestinationPath, edge.DestinationName);
                if (!visited.Add(edgeDestination)) continue;
                var next = new List<CoordinateTransformEdge>(path) { edge };
                queue.Enqueue((edgeDestination, next));
            }
        }

        throw new InvalidOperationException(
            $"No transformation path from '{sourceKey}' to '{destinationKey}'.");
    }

    /// <summary>Transforms a point by following the shortest available graph path.</summary>
    public double[] TransformPoint(
        IReadOnlyList<double> point,
        CoordinateSystemReference source,
        CoordinateSystemReference destination)
    {
        var result = point.ToArray();
        foreach (var edge in FindPath(source, destination))
            result = edge.Reversed
                ? CoordinateTransformationEvaluator.TransformInverse(edge.Transformation, result)
                : CoordinateTransformationEvaluator.TransformForward(edge.Transformation, result);
        return result;
    }

    private IEnumerable<CoordinateTransformEdge> EdgesFrom(CoordinateSystemKey source)
    {
        foreach (var transformation in _transformations)
        {
            if (transformation.Input?.Name is null || transformation.Output?.Name is null)
                continue;
            var input = CoordinateSystemKey.From(transformation.Input);
            var output = CoordinateSystemKey.From(transformation.Output);
            if (input == source)
                yield return new CoordinateTransformEdge(transformation, output.Path, output.Name, Reversed: false);
            if (output == source)
                yield return new CoordinateTransformEdge(transformation, input.Path, input.Name, Reversed: true);
        }
    }

    private readonly record struct CoordinateSystemKey(string Path, string Name)
    {
        public static CoordinateSystemKey From(CoordinateSystemReference reference) => new(
            reference.Path?.Trim('/') ?? string.Empty,
            reference.Name ?? throw new ArgumentException("Coordinate system reference is missing 'name'.", nameof(reference)));

        public override string ToString() => string.IsNullOrEmpty(Path) ? Name : $"{Path}:{Name}";
    }
}

/// <summary>A directed traversal through one scene transformation.</summary>
public readonly record struct CoordinateTransformEdge(
    CoordinateTransformation Transformation,
    string DestinationPath,
    string DestinationName,
    bool Reversed);
