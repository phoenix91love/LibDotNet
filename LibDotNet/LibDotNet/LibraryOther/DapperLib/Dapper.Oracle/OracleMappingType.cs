namespace Internal.Dapper
{
    /// <summary>
    /// BulkMapping enum to map Collection type for parameter when using PL/Sql associative arrays without referencing oracle directly.
    /// </summary>
    public enum OracleMappingCollectionType
    {
        None,
        PLSQLAssociativeArray,
    }

    /// <summary>
    /// BulkMapping enum to map Parameter status for OracleParameter
    /// </summary>
    public enum OracleParameterMappingStatus
    {
        Success,
        NullFetched,
        NullInsert,
        Truncation,
    }
}
