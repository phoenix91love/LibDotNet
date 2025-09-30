using LibDotNet.DBHelpers;
using LibDotNet.Helper;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Test
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            
            const string sqlServerConn = "Server=.;Database=Test;Trusted_Connection=true;";
            LogWriters<Program>.Info(sqlServerConn);
            LogWriters<regions>.Info(sqlServerConn);
            LogWriters<Program>.Info(sqlServerConn);
            LogWriters<regions>.Info(sqlServerConn);
            string connectionOracle = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.1.99)(PORT=1528))(CONNECT_DATA=(SERVICE_NAME=DATATEST)));User Id=system;Password=admin123;";
            // Query đơn giản
            try
            {
                var users = await DatabaseAccess<DatabaseOracle>
                .QueryAsync<regions, regions, regions>(connectionOracle, "USERSAPAPP.GetRegion");
            }
            catch (System.Exception ex)
            {


            }



            // Query với stored procedure
            //var user = await DatabaseAccess<DatabaseMySql>
            //    .QueryFirstOrDefaultAsync<User>(sqlServerConn, "sp_GetUserById",
            //    new { UserId = 1 }, commandType: CommandType.StoredProcedure);


            //// Cách 2: Sử dụng với transaction scope tường minh
            //var success = await DatabaseAccess<DatabaseSqlServer>.ExecuteInTransactionAsync(sqlServerConn,
            //    async transactionScope =>
            //    {
            //        // Thực hiện multiple operations
            //        var userId = await transactionScope.ExecuteAsync(
            //            "INSERT INTO Users (Name, Email) VALUES (@Name, @Email); SELECT SCOPE_IDENTITY();",
            //            new { Name = "Jane", Email = "jane@email.com" });

            //        var user = await transactionScope.QueryFirstOrDefaultAsync<User>(
            //            "SELECT * FROM Users WHERE Id = @Id",
            //            new { Id = userId });

            //        await transactionScope.ExecuteAsync(
            //            "INSERT INTO UserLogs (UserId, Action) VALUES (@UserId, @Action)",
            //            new { UserId = userId, Action = "Created" });

            //        return user != null;
            //    });
        }

    }
    public class regions
    {
        public int Region_id { get; set; }
        public string region_name { get; set; }

    }
}
