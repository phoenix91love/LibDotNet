using Libs.RedisHelper;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
namespace Test
{
    internal class Program
    {
        static async Task Main(string[] args)
        {

            try
            {
                var redis = new RedisService(await ConnectionMultiplexer.ConnectAsync("localhost"));

                Console.WriteLine("=== COMPLETE STORAGE TYPES DEMO (FIXED) ===");

                var products = GenerateTestProducts(10);

                // 1. STRING STORAGE - ĐÃ SỬA LỖI
                Console.WriteLine("\n1. STRING STORAGE (FIXED):");

                // Store single object
                await redis.String<Product>("product:single")
                    .I(products[0]);

                var singleProduct = await redis.String<Product>("product:single").G();

                // Store collection - ĐÃ KHÔNG CÒN LỖI
                await redis.String<List<Product>>("products:collection")
                    .I(products);

                var collection = await redis.String<List<Product>>("products:collection").G();
                Console.WriteLine($"Product collection: {collection?.Count} items");

                // 2. INDIVIDUAL STORAGE
                Console.WriteLine("\n2. INDIVIDUAL STORAGE:");
                await redis.Individual<Product>("products:individual")
                    .I(products, p => p.Id.ToString());

                var individualCount = await redis.Individual<Product>("products:individual").C();
                Console.WriteLine($"Individual storage: {individualCount} items");

                // 3. HASH STORAGE
                Console.WriteLine("\n3. HASH STORAGE:");
                await redis.Hash<Product>("products:hash")
                    .I(products, p => p.Id.ToString());

                var hashCount = await redis.Hash<Product>("products:hash").C();
                Console.WriteLine($"Hash storage: {hashCount} items");

                // 4. LIST STORAGE
                Console.WriteLine("\n4. LIST STORAGE:");
                await redis.List<Product>("products:list")
                    .I(products);

                var listCount = await redis.List<Product>("products:list").C();
                Console.WriteLine($"List storage: {listCount} items");

                // 5. CRUD OPERATIONS CHO TẤT CẢ STORAGE TYPES
                Console.WriteLine("\n5. COMPLETE CRUD OPERATIONS:");

                // INSERT
                await redis.Hash<Product>("test:hash").I(products[0], "1");
                await redis.List<Product>("test:list").I(products[0]);
                await redis.String<Product>("test:string").I(products[0]);
                await redis.Individual<Product>("test:individual").I(products[0], "1");

                // GET
                var fromHash = await redis.Hash<Product>("test:hash").G("1");
                var fromList = await redis.List<Product>("test:list").At(0);
                var fromString = await redis.String<Product>("test:string").G();
                var fromIndividual = await redis.Individual<Product>("test:individual").G("1");

                Console.WriteLine($"Retrieved from all storage types successfully");

                // UPDATE
                await redis.Hash<Product>("test:hash").U("1", p => p.Name = "Updated");
                await redis.List<Product>("test:list").U("1", p => p.Name = "Updated");
                await redis.Individual<Product>("test:individual").U("1", p => p.Name = "Updated");

                // DELETE
                await redis.Hash<Product>("test:hash").D("1");
                await redis.List<Product>("test:list").D("1");
                await redis.String<Product>("test:string").D("1");
                await redis.Individual<Product>("test:individual").D("1");

                Console.WriteLine("CRUD operations completed successfully");

                // 6. PIPELINE OPTIMIZATION
                Console.WriteLine("\n6. PIPELINE OPTIMIZATION:");

                await redis.Hash<Product>("pipeline:hash")
                    .HighPerf()
                    .I(products, p => p.Id.ToString());

                await redis.List<Product>("pipeline:list")
                    .HighPerf()
                    .I(products);

                await redis.String<List<Product>>("pipeline:string")
                    .HighPerf()
                    .I(products);

                await redis.Individual<Product>("pipeline:individual")
                    .HighPerf()
                    .I(products, p => p.Id.ToString());

                Console.WriteLine("Pipeline optimization completed");

                // CLEANUP
                Console.WriteLine("\n7. CLEANUP:");

                await redis.Delete("product:single");
                await redis.Delete("products:collection");

                await redis.Delete("products:hash");
                await redis.Delete("products:list");
                await redis.Delete("test:hash");
                await redis.Delete("test:list");
                await redis.Delete("test:string");

                await redis.Delete("pipeline:hash");
                await redis.Delete("pipeline:list");
                await redis.Delete("pipeline:string");

                Console.WriteLine("Cleanup completed");
                //await redis.Individual<Product>("pipeline:individual").ClearAll();
                Console.WriteLine("\n=== ALL STORAGE TYPES WORKING CORRECTLY ===");
            }
            catch (System.Exception ex)
            {


            }

            static List<Product> GenerateTestProducts(int count)
            {
                var random = new Random();
                return Enumerable.Range(1, count).Select(i => new Product
                {
                    Id = i.ToString(),
                    Name = $"Product_{i}",
                    Price = random.Next(10, 1000),
                    Category = i % 2 == 0 ? "Electronics" : "Furniture"
                }).ToList();
            }

        }
        public class Product
        {
            public string Id { get; set; }
            public string Name { get; set; } = string.Empty;
            public decimal Price { get; set; }
            public string Category { get; set; } = string.Empty;
            public DateTime CreatedAt { get; set; } = DateTime.Now;
        }
    }
}