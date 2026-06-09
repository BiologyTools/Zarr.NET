namespace ZarrNET;

/// <summary>
/// Typed representation of a Zarr v3 data type string (e.g. "uint8", "float32", "complex64").
/// Resolves element size in bytes and provides type classification used
/// by the codec pipeline for byte-order handling.
/// </summary>
public sealed class ZarrDataType
{
    public string TypeString { get; }
    public int    ElementSize { get; }
    public int    ByteOrderElementSize { get; }
    public bool   IsFloat     { get; }
    public bool   IsInteger   { get; }
    public bool   IsComplex   { get; }
    public bool   IsSigned    { get; }

    private ZarrDataType(
        string typeString,
        int elementSize,
        int byteOrderElementSize,
        bool isFloat,
        bool isComplex,
        bool isSigned)
    {
        TypeString           = typeString;
        ElementSize          = elementSize;
        ByteOrderElementSize = byteOrderElementSize;
        IsFloat              = isFloat;
        IsComplex            = isComplex;
        IsInteger            = !isFloat && !isComplex;
        IsSigned             = isSigned;
    }

    private static ZarrDataType Numeric(
        string typeString,
        int elementSize,
        bool isFloat,
        bool isSigned)
        => new(
            typeString,
            elementSize,
            byteOrderElementSize: elementSize,
            isFloat,
            isComplex: false,
            isSigned);

    private static ZarrDataType Complex(
        string typeString,
        int elementSize,
        int componentSize)
        => new(
            typeString,
            elementSize,
            byteOrderElementSize: componentSize,
            isFloat: false,
            isComplex: true,
            isSigned: true);

    // -------------------------------------------------------------------------
    // Parsing
    // -------------------------------------------------------------------------

    public static ZarrDataType Parse(string typeString)
    {
        return typeString switch
        {
            "bool"       => Numeric(typeString, 1, isFloat: false, isSigned: false),
            "int8"       => Numeric(typeString, 1, isFloat: false, isSigned: true),
            "uint8"      => Numeric(typeString, 1, isFloat: false, isSigned: false),
            "int16"      => Numeric(typeString, 2, isFloat: false, isSigned: true),
            "uint16"     => Numeric(typeString, 2, isFloat: false, isSigned: false),
            "int32"      => Numeric(typeString, 4, isFloat: false, isSigned: true),
            "uint32"     => Numeric(typeString, 4, isFloat: false, isSigned: false),
            "int64"      => Numeric(typeString, 8, isFloat: false, isSigned: true),
            "uint64"     => Numeric(typeString, 8, isFloat: false, isSigned: false),
            "float32"    => Numeric(typeString, 4, isFloat: true,  isSigned: true),
            "float64"    => Numeric(typeString, 8, isFloat: true,  isSigned: true),
            "complex64"  => Complex(typeString, 8, componentSize: 4),
            "complex128" => Complex(typeString, 16, componentSize: 8),
            _ => throw new NotSupportedException($"Unsupported Zarr data type: '{typeString}'")
        };
    }

    public override string ToString() => TypeString;
}
