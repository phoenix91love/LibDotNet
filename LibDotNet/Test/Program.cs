using Libs.DBHelpers;
using Libs.Images;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using System.Threading.Tasks;
namespace Test
{
    internal class Program
    {
        static async Task Main(string[] args)
        {

            const string sqlServerConn = "Server=.;Database=Test;Trusted_Connection=true;";




            string connectionOracle = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.1.99)(PORT=1528))(CONNECT_DATA=(SERVICE_NAME=DATATEST)));User Id=system;Password=admin123;";
            // Query đơn giản
            try
            {
                var image = Image.FromFile(@"C:\Users\phoen\Downloads\evernight-honkai-5120x2880-24123.jpg");

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


            // Cách 2: Sử dụng với transaction scope tường minh
            var success = await DatabaseAccess<DatabaseOracle>.ExecuteInTransactionAsync<regions>(sqlServerConn,
                async transactionScope =>
                {
                    // Thực hiện multiple operations
                    var userId = await transactionScope.ExecuteAsync(
                        "INSERT INTO Users (Name, Email) VALUES (@Name, @Email); SELECT SCOPE_IDENTITY();",
                        new { Name = "Jane", Email = "jane@email.com" });

                    var user = await transactionScope.QueryAsync<regions, regions, regions>(
                        "SELECT * FROM Users WHERE Id = @Id",
                        new { Id = userId });

                    await transactionScope.ExecuteAsync(
                        "INSERT INTO UserLogs (UserId, Action) VALUES (@UserId, @Action)",
                        new { UserId = userId, Action = "Created" });

                    return new regions();
                });
        }
        public static byte[] CopyImageToByteArray(Image theImage)
        {
            using (MemoryStream memoryStream = new MemoryStream())
            {
                theImage.Save(memoryStream, theImage.RawFormat);
                return memoryStream.ToArray();
            }
        }
    }
    public class regions
    {
        public int Region_id { get; set; }
        public string region_name { get; set; }

    }
}
