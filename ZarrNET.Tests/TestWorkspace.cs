namespace ZarrNET.Tests;

internal sealed class TestWorkspace : IDisposable
{
    public string Path { get; } = System.IO.Path.Combine(
        System.IO.Path.GetTempPath(), "ZarrNET.Tests", Guid.NewGuid().ToString("N"));

    public TestWorkspace() => Directory.CreateDirectory(Path);

    public void Dispose()
    {
        if (Directory.Exists(Path))
            Directory.Delete(Path, recursive: true);
    }
}
