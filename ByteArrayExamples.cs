using OmeZarr.Core.OmeZarr;
using OmeZarr.Core.OmeZarr.Helpers;

// =============================================================================
// Getting byte[] from PlaneResult - various methods
// =============================================================================

async Task ByteArrayExamples(string zarrPath)
{
    await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
    var image = reader.AsMultiscaleImage();
    var level = await image.OpenResolutionLevelAsync(0);

    var plane = await level.ReadPlaneAsync(t: 0, c: 1, z: 5);

    // -------------------------------------------------------------------------
    // Method 1: Get raw bytes directly (native format from Zarr)
    // -------------------------------------------------------------------------
    
    byte[] rawBytes = plane.GetRawBytes();
    // or simply:
    byte[] rawBytes2 = plane.Data;
    
    Console.WriteLine($"Raw bytes: {rawBytes.Length} bytes");
    // For uint16 data, this is raw 2-byte little-endian values

    // -------------------------------------------------------------------------
    // Method 2: Convert to specific pixel format
    // -------------------------------------------------------------------------
    
    // Convert uint16 to 8-bit grayscale (common for display)
    byte[] gray8 = plane.ToBytes<ushort>(PixelFormat.Gray8);
    Console.WriteLine($"Gray8: {gray8.Length} bytes ({plane.Width}x{plane.Height} pixels)");
    
    // Convert to 16-bit grayscale (preserves full dynamic range)
    byte[] gray16 = plane.ToBytes<ushort>(PixelFormat.Gray16);
    Console.WriteLine($"Gray16: {gray16.Length} bytes");
    
    // Convert to BGR24 (for OpenCV, GDI+, etc.)
    byte[] bgr24 = plane.ToBytes<ushort>(PixelFormat.Bgr24);
    Console.WriteLine($"BGR24: {bgr24.Length} bytes");
    
    // Convert to BGRA32 (for WPF BitmapSource, etc.)
    byte[] bgra32 = plane.ToBytes<ushort>(PixelFormat.Bgra32);
    Console.WriteLine($"BGRA32: {bgra32.Length} bytes");

    // -------------------------------------------------------------------------
    // Method 3: Manual conversion from typed array
    // -------------------------------------------------------------------------
    
    ushort[] pixels = plane.As1DArray<ushort>();
    
    // Convert to byte[] manually with custom logic
    byte[] customBytes = new byte[pixels.Length];
    for (int i = 0; i < pixels.Length; i++)
    {
        // Custom normalization - e.g., windowing for medical images
        ushort value = pixels[i];
        double normalized = Math.Clamp((value - 100) / 4000.0, 0, 1);
        customBytes[i] = (byte)(normalized * 255);
    }
}

// =============================================================================
// Save to image file using System.Drawing (Windows)
// =============================================================================

async Task SavePlaneAsImage(string zarrPath, string outputPath)
{
    await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
    var image = reader.AsMultiscaleImage();
    var level = await image.OpenResolutionLevelAsync(0);

    var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 5);
    
    // Get as 8-bit grayscale
    byte[] pixels = plane.ToBytes<ushort>(PixelFormat.Gray8);
    
    // Create bitmap (System.Drawing - Windows only)
    using var bitmap = new System.Drawing.Bitmap(plane.Width, plane.Height, 
        System.Drawing.Imaging.PixelFormat.Format8bppIndexed);
    
    // Set grayscale palette
    var palette = bitmap.Palette;
    for (int i = 0; i < 256; i++)
        palette.Entries[i] = System.Drawing.Color.FromArgb(i, i, i);
    bitmap.Palette = palette;
    
    // Copy pixels
    var bitmapData = bitmap.LockBits(
        new System.Drawing.Rectangle(0, 0, plane.Width, plane.Height),
        System.Drawing.Imaging.ImageLockMode.WriteOnly,
        System.Drawing.Imaging.PixelFormat.Format8bppIndexed);
    
    System.Runtime.InteropServices.Marshal.Copy(
        pixels, 0, bitmapData.Scan0, pixels.Length);
    
    bitmap.UnlockBits(bitmapData);
    bitmap.Save(outputPath, System.Drawing.Imaging.ImageFormat.Png);
    
    Console.WriteLine($"Saved plane to {outputPath}");
}

// =============================================================================
// Create WPF BitmapSource (for WPF apps)
// =============================================================================

#if WINDOWS
async Task<System.Windows.Media.Imaging.BitmapSource> CreateWpfBitmap(string zarrPath)
{
    await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
    var image = reader.AsMultiscaleImage();
    var level = await image.OpenResolutionLevelAsync(0);

    var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);
    
    // Convert to BGRA32 for WPF
    byte[] pixels = plane.ToBytes<ushort>(PixelFormat.Bgra32);
    
    // Create BitmapSource
    var bitmap = System.Windows.Media.Imaging.BitmapSource.Create(
        plane.Width,
        plane.Height,
        96, 96,  // DPI
        System.Windows.Media.PixelFormats.Bgra32,
        null,  // No palette for BGRA32
        pixels,
        plane.Width * 4  // Stride (4 bytes per pixel)
    );
    
    bitmap.Freeze();  // Make it thread-safe
    return bitmap;
}
#endif

// =============================================================================
// OpenCV interop (using OpenCvSharp)
// =============================================================================

#if OPENCV_AVAILABLE
async Task ProcessWithOpenCV(string zarrPath)
{
    await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
    var image = reader.AsMultiscaleImage();
    var level = await image.OpenResolutionLevelAsync(0);

    var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);
    
    // OpenCV expects BGR or grayscale
    byte[] pixels = plane.ToBytes<ushort>(PixelFormat.Gray8);
    
    // Create OpenCV Mat
    var mat = new OpenCvSharp.Mat(plane.Height, plane.Width, 
        OpenCvSharp.MatType.CV_8UC1, pixels);
    
    // Process with OpenCV
    var blurred = new OpenCvSharp.Mat();
    OpenCvSharp.Cv2.GaussianBlur(mat, blurred, new OpenCvSharp.Size(5, 5), 0);
    
    // Get bytes back
    byte[] processedBytes = new byte[blurred.Total() * blurred.ElemSize()];
    System.Runtime.InteropServices.Marshal.Copy(
        blurred.Data, processedBytes, 0, processedBytes.Length);
    
    mat.Dispose();
    blurred.Dispose();
}
#endif

// =============================================================================
// SkiaSharp interop (cross-platform image processing)
// =============================================================================

#if SKIASHARP_AVAILABLE
async Task SaveWithSkiaSharp(string zarrPath, string outputPath)
{
    await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
    var image = reader.AsMultiscaleImage();
    var level = await image.OpenResolutionLevelAsync(0);

    var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);
    
    // SkiaSharp uses BGRA
    byte[] pixels = plane.ToBytes<ushort>(PixelFormat.Bgra32);
    
    // Create SKBitmap
    var bitmap = new SkiaSharp.SKBitmap(
        plane.Width, 
        plane.Height, 
        SkiaSharp.SKColorType.Bgra8888, 
        SkiaSharp.SKAlphaType.Opaque);
    
    System.Runtime.InteropServices.Marshal.Copy(
        pixels, 0, bitmap.GetPixels(), pixels.Length);
    
    // Save as PNG
    using var image = SkiaSharp.SKImage.FromBitmap(bitmap);
    using var data = image.Encode(SkiaSharp.SKEncodedImageFormat.Png, 100);
    using var stream = System.IO.File.OpenWrite(outputPath);
    data.SaveTo(stream);
    
    Console.WriteLine($"Saved with SkiaSharp to {outputPath}");
}
#endif

// =============================================================================
// ImageSharp interop (cross-platform, modern)
// =============================================================================

#if IMAGESHARP_AVAILABLE
async Task ProcessWithImageSharp(string zarrPath, string outputPath)
{
    await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
    var image = reader.AsMultiscaleImage();
    var level = await image.OpenResolutionLevelAsync(0);

    var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);
    
    // ImageSharp can work with various formats
    byte[] pixels = plane.ToBytes<ushort>(PixelFormat.Gray8);
    
    // Create Image<L8> (8-bit grayscale)
    var img = SixLabors.ImageSharp.Image.LoadPixelData<SixLabors.ImageSharp.PixelFormats.L8>(
        pixels, plane.Width, plane.Height);
    
    // Process with ImageSharp
    img.Mutate(x => x.GaussianBlur(2.0f));
    
    // Save
    img.SaveAsPng(outputPath);
    
    Console.WriteLine($"Processed and saved with ImageSharp to {outputPath}");
}
#endif

// =============================================================================
// Direct memory copy for maximum performance
// =============================================================================

unsafe void CopyToUnmanagedBuffer(PlaneResult plane)
{
    byte[] data = plane.GetRawBytes();
    
    // Allocate unmanaged memory
    IntPtr unmanagedPtr = System.Runtime.InteropServices.Marshal.AllocHGlobal(data.Length);
    
    try
    {
        // Fast copy to unmanaged memory
        System.Runtime.InteropServices.Marshal.Copy(data, 0, unmanagedPtr, data.Length);
        
        // Use unmanaged memory (e.g., pass to native code)
        // YourNativeFunction(unmanagedPtr, plane.Width, plane.Height);
    }
    finally
    {
        // Always free unmanaged memory
        System.Runtime.InteropServices.Marshal.FreeHGlobal(unmanagedPtr);
    }
}

// =============================================================================
// Batch processing - write multiple planes to a single file
// =============================================================================

async Task WritePlaneStack(string zarrPath, string outputPath)
{
    await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
    var image = reader.AsMultiscaleImage();
    var level = await image.OpenResolutionLevelAsync(0);
    
    var axes = level.Multiscale.Axes;
    var zIndex = Array.FindIndex(axes, a => a.Name.Equals("z", StringComparison.OrdinalIgnoreCase));
    var numSlices = (int)level.Shape[zIndex];
    
    using var fileStream = System.IO.File.Create(outputPath);
    using var writer = new System.IO.BinaryWriter(fileStream);
    
    // Write header
    var axes = level.Multiscale.Axes;
    var yIndex = Array.FindIndex(axes, a => a.Name.Equals("y", StringComparison.OrdinalIgnoreCase));
    var xIndex = Array.FindIndex(axes, a => a.Name.Equals("x", StringComparison.OrdinalIgnoreCase));
    
    var width = (int)level.Shape[xIndex];
    var height = (int)level.Shape[yIndex];
    
    writer.Write(width);
    writer.Write(height);
    writer.Write(numSlices);
    
    // Write each z-slice
    for (int z = 0; z < numSlices; z++)
    {
        var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: z);
        byte[] bytes = plane.ToBytes<ushort>(PixelFormat.Gray16);
        writer.Write(bytes);
    }
    
    Console.WriteLine($"Wrote {numSlices} planes to {outputPath}");
}
