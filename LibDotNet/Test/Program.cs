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

                Console.WriteLine("=== ALL STORAGE TYPES EXPIRY DEMO ===");

                var products = GenerateTestProducts(3);

                // 1. HASH STORAGE với EXPIRY
                Console.WriteLine("\n1. HASH STORAGE với EXPIRY:");

                await redis.Hash<Product>("demo:hash:expiry")
                    .Session() // 2 hours sliding
                    .Insert(products, p => p.Id.ToString());

                var hashTTL = await redis.Hash<Product>("demo:hash:expiry").TTL();
                Console.WriteLine($"Hash storage TTL: {hashTTL?.TotalSeconds} seconds");

                // 2. LIST STORAGE với EXPIRY
                Console.WriteLine("\n2. LIST STORAGE với EXPIRY:");

                await redis.List<Product>("demo:list:expiry")
                    .Cache() // 10 minutes fixed
                    .Insert(products);

                var listTTL = await redis.List<Product>("demo:list:expiry").TTLKey("") ;
                Console.WriteLine($"List storage TTL: {listTTL?.TotalSeconds} seconds");

                // 3. STRING STORAGE với EXPIRY
                Console.WriteLine("\n3. STRING STORAGE với EXPIRY:");

                // Single object
                await redis.String<Product>("demo:string:single:expiry")
                    .ShortLived() // 30 minutes sliding
                    .Insert(products[0]);

                var stringSingleTTL = await redis.String<Product>("demo:string:single:expiry").TTL();
                Console.WriteLine($"String single TTL: {stringSingleTTL?.TotalSeconds} seconds");

                // Collection
                await redis.String<List<Product>>("demo:string:collection:expiry")
                    .Expiry(TimeSpan.FromHours(1))
                    .Insert(products);

                var stringCollectionTTL = await redis.String<List<Product>>("demo:string:collection:expiry").HighPerf().TTL();
                Console.WriteLine($"String collection TTL: {stringCollectionTTL?.TotalSeconds} seconds");

                // 4. INDIVIDUAL STORAGE với EXPIRY
                Console.WriteLine("\n4. INDIVIDUAL STORAGE với EXPIRY:");

                await redis.Individual<Product>("demo:individual:expiry")
                    .Session()
                    .Insert(products, p => p.Id.ToString());

                var individualKeyTTL = await redis.Individual<Product>("demo:individual:expiry").TTLKey("1");
                Console.WriteLine($"Individual key TTL: {individualKeyTTL?.TotalSeconds} seconds");

                // 5. SLIDING EXPIRY VERIFICATION
                Console.WriteLine("\n5. SLIDING EXPIRY VERIFICATION:");

                var initialHashTTL = await redis.Hash<Product>("demo:hash:expiry").TTL();
                Console.WriteLine($"Initial hash TTL: {initialHashTTL?.TotalSeconds} seconds");

                // Access data - should renew expiry
                await Task.Delay(1000);
                var data = await redis.Hash<Product>("demo:hash:expiry").Get("1");
                var afterAccessTTL = await redis.Hash<Product>("demo:hash:expiry").TTL();

                Console.WriteLine($"After access TTL: {afterAccessTTL?.TotalSeconds} seconds");
                Console.WriteLine($"Sliding expiry working: {afterAccessTTL?.TotalSeconds > initialHashTTL?.TotalSeconds}");

                // 6. FIXED EXPIRY VERIFICATION (List storage)
                Console.WriteLine("\n6. FIXED EXPIRY VERIFICATION:");

                var initialListTTL = await redis.List<Product>("demo:list:expiry").TTL();
                Console.WriteLine($"Initial list TTL: {initialListTTL?.TotalSeconds} seconds");

                // Access data - should NOT renew expiry (fixed)
                await Task.Delay(1000);
                var listData = await redis.List<Product>("demo:list:expiry").GetAll();
                var afterListAccessTTL = await redis.List<Product>("demo:list:expiry").TTL();

                Console.WriteLine($"After list access TTL: {afterListAccessTTL?.TotalSeconds} seconds");
                Console.WriteLine($"Fixed expiry maintained: {afterListAccessTTL?.TotalSeconds < initialListTTL?.TotalSeconds}");

                // 7. EXPIRY MAINTENANCE ON OPERATIONS
                Console.WriteLine("\n7. EXPIRY MAINTENANCE ON OPER");
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