using System.Text.Json;
using ZarrNET.Core;
using ZarrNET.Core.OmeZarr.Metadata;
using Xunit;

namespace ZarrNET.Tests;

public sealed class SceneReaderTests
{
    [Fact]
    public async Task SceneRoot_DetectsReferencesAndOpensFirstMultiscaleImage()
    {
        using var workspace = new TestWorkspace();
        Directory.CreateDirectory(Path.Combine(workspace.Path, "input"));
        Directory.CreateDirectory(Path.Combine(workspace.Path, "output"));

        await File.WriteAllTextAsync(Path.Combine(workspace.Path, "zarr.json"), SceneJson);
        await File.WriteAllTextAsync(
            Path.Combine(workspace.Path, "input", "zarr.json"),
            MultiscaleJson("input-image"));
        await File.WriteAllTextAsync(
            Path.Combine(workspace.Path, "output", "zarr.json"),
            MultiscaleJson("output-image"));

        await using var reader = await OmeZarrReader.OpenAsync(workspace.Path);

        Assert.Equal(OmeAttributesParser.OmeNodeType.Scene, reader.RootNodeType);
        Assert.Equal("0.6", reader.NgffVersion);

        var scene = reader.AsScene();
        Assert.Equal(new[] { "input", "output" }, scene.ImagePaths);

        var image = await reader.AsMultiscaleImageAsync();
        Assert.Equal("input", image.Path);
        Assert.Equal("input-image", Assert.Single(image.Multiscales).Name);
    }

    private const string SceneJson = """
        {
          "zarr_format": 3,
          "node_type": "group",
          "attributes": {
            "ome": {
              "version": "0.6",
              "scene": {
                "coordinateTransformations": [{
                  "type": "bijection",
                  "input": { "name": "physical", "path": "input" },
                  "output": { "name": "physical", "path": "output" },
                  "forward": {
                    "type": "sequence",
                    "transformations": [
                      { "type": "affine", "affine": [[1, 0, 0], [0, 1, 0]] }
                    ]
                  }
                }]
              }
            }
          }
        }
        """;

    private static string MultiscaleJson(string name) => $$"""
        {
          "zarr_format": 3,
          "node_type": "group",
          "attributes": {
            "ome": {
              "version": "0.6",
              "multiscales": [{
                "name": "{{name}}",
                "coordinateSystems": [{
                  "name": "physical",
                  "axes": [
                    { "name": "y", "type": "space" },
                    { "name": "x", "type": "space" }
                  ]
                }],
                "datasets": [{
                  "path": "s0",
                  "coordinateTransformations": [{
                    "type": "scale",
                    "scale": [1, 1],
                    "input": { "path": "s0" },
                    "output": { "name": "physical" }
                  }]
                }]
              }]
            }
          }
        }
        """;
}
