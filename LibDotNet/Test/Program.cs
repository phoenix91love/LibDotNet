using LibDotNet.DBHelpers;
using Mysqlx.Crud;
using System;
using System.Data;
using System.Threading.Tasks;

namespace Test
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            const string sqlServerConn = "Server=.;Database=Test;Trusted_Connection=true;";

            // Query đơn giản
            var users = await DatabaseAccess<DatabaseSqlServer>
                .QueryAsync<User>(sqlServerConn, "SELECT * FROM Users WHERE Active = @Active",
                new { Active = true });

            // Query với stored procedure
            var user = await DatabaseAccess<DatabaseMySql>
                .QueryFirstOrDefaultAsync<User>(sqlServerConn, "sp_GetUserById",
                new { UserId = 1 }, commandType: CommandType.StoredProcedure);
            

            // Cách 2: Sử dụng với transaction scope tường minh
            var success = await DatabaseAccess<DatabaseSqlServer>.ExecuteInTransactionAsync(sqlServerConn,
                async transactionScope =>
                {
                    // Thực hiện multiple operations
                    var userId = await transactionScope.ExecuteAsync(
                        "INSERT INTO Users (Name, Email) VALUES (@Name, @Email); SELECT SCOPE_IDENTITY();",
                        new { Name = "Jane", Email = "jane@email.com" });

                    var user = await transactionScope.QueryFirstOrDefaultAsync<User>(
                        "SELECT * FROM Users WHERE Id = @Id",
                        new { Id = userId });

                    await transactionScope.ExecuteAsync(
                        "INSERT INTO UserLogs (UserId, Action) VALUES (@UserId, @Action)",
                        new { UserId = userId, Action = "Created" });

                    return user != null;
                });
        }

    }
    public class User
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Email { get; set; }
        public int Age { get; set; }
    }
}
