using System;
using System.Collections.Generic;
using System.Text;

namespace ZarrNET.Core
{
    public struct ZCT : IEquatable<ZCT>
    {
        public int Z, C, T;
        public ZCT(int z, int c, int t)
        {
            Z = z;
            C = c;
            T = t;
        }
        public static bool operator ==(ZCT c1, ZCT c2)
        {
            if (c1.Z == c2.Z && c1.C == c2.C && c1.T == c2.T)
                return true;
            else
                return false;
        }
        public static bool operator !=(ZCT c1, ZCT c2)
        {
            if (c1.Z == c2.Z && c1.C == c2.C && c1.T == c2.T)
                return false;
            else
                return true;
        }

        public override bool Equals(object? obj)
        {
            return obj is ZCT other && Equals(other);
        }

        public bool Equals(ZCT other)
        {
            return Z == other.Z && C == other.C && T == other.T;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Z, C, T);
        }

        public override string ToString()
        {
            return Z + "," + C + "," + T;
        }
    }
}
