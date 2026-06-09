namespace ZarrNET;

/// <summary>
/// Parses numpy-style dtype strings used in Zarr v2.
/// Format: [byteorder][typecode][size]
///   byteorder: '<' (little), '>' (big), '|' (not applicable)
///   typecode:  'u' (uint), 'i' (int), 'f' (float), 'c' (complex), 'b' (bool)
///   size:      number of bytes (1, 2, 4, 8, 16)
/// Examples: "<u2" → uint16, ">f4" → big-endian float32, "<c8" → complex64, "<c16" → complex128
/// </summary>
public static class NumpyDtypeParser
{
    public static (ZarrDataType DataType, ByteOrder ByteOrder) Parse(string dtype)
    {
        if (string.IsNullOrEmpty(dtype) || dtype.Length < 2)
            throw new ArgumentException($"Invalid numpy dtype string: '{dtype}'");

        var byteOrderChar = dtype[0];
        var typeCode      = dtype[1];
        var sizeStr       = dtype[2..];

        if (!int.TryParse(sizeStr, out var size))
            throw new ArgumentException($"Cannot parse size from dtype '{dtype}'");

        var byteOrder = ParseByteOrder(byteOrderChar);
        var dataType  = ParseDataType(typeCode, size);

        return (dataType, byteOrder);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static ByteOrder ParseByteOrder(char byteOrderChar)
    {
        return byteOrderChar switch
        {
            '<' => ByteOrder.LittleEndian,
            '>' => ByteOrder.BigEndian,
            '|' => ByteOrder.LittleEndian,  // byte-order not applicable (e.g. uint8)
            '=' => BitConverter.IsLittleEndian
                    ? ByteOrder.LittleEndian
                    : ByteOrder.BigEndian,   // native byte order
            _   => throw new NotSupportedException(
                       $"Unsupported byte order character: '{byteOrderChar}'")
        };
    }

    private static ZarrDataType ParseDataType(char typeCode, int size)
    {
        return (typeCode, size) switch
        {
            ('b', 1) => ZarrDataType.Parse("bool"),
            ('u', 1) => ZarrDataType.Parse("uint8"),
            ('i', 1) => ZarrDataType.Parse("int8"),
            ('u', 2) => ZarrDataType.Parse("uint16"),
            ('i', 2) => ZarrDataType.Parse("int16"),
            ('u', 4) => ZarrDataType.Parse("uint32"),
            ('i', 4) => ZarrDataType.Parse("int32"),
            ('u', 8) => ZarrDataType.Parse("uint64"),
            ('i', 8) => ZarrDataType.Parse("int64"),
            ('f', 4) => ZarrDataType.Parse("float32"),
            ('f', 8) => ZarrDataType.Parse("float64"),
            ('c', 8) => ZarrDataType.Parse("complex64"),
            ('c', 16) => ZarrDataType.Parse("complex128"),
            _        => throw new NotSupportedException(
                            $"Unsupported numpy dtype: type='{typeCode}', size={size} bytes")
        };
    }
}
