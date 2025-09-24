using System;
using System.Collections.Generic;
using System.Text;

namespace LibDotNet.DBHelpers
{
    public enum DatabaseTypes
    {
        SqlServer,
        MySql,
        PostgreSql,
        Oracle
    }
    public abstract class DatabaseTypeBase
    {
        public abstract DatabaseTypes Type { get; }
    }

    public class DatabaseSqlServer : DatabaseTypeBase
    {
        public override DatabaseTypes Type => DatabaseTypes.SqlServer;
    }

    public class DatabaseMySql : DatabaseTypeBase
    {
        public override DatabaseTypes Type => DatabaseTypes.MySql;
    }

    public class DatabasePostgreSql : DatabaseTypeBase
    {
        public override DatabaseTypes Type => DatabaseTypes.PostgreSql;
    }

    public class OracleDatabase : DatabaseTypeBase
    {
        public override DatabaseTypes Type => DatabaseTypes.Oracle;
    }
}
