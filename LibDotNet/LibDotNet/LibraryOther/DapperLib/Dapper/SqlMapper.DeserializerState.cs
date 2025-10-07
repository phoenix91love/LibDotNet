using System;
using System.Data.Common;

namespace Internal.Dapper
{
    internal static partial class SqlMapper
    {
        private readonly struct DeserializerState
        {
            public readonly int Hash;
            public readonly Func<DbDataReader, object> Func;

            public DeserializerState(int hash, Func<DbDataReader, object> func)
            {
                Hash = hash;
                Func = func;
            }
        }
    }
}
