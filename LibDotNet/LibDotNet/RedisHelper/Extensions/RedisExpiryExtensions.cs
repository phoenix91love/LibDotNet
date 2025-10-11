using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Libs.RedisHelper
{
    public static class RedisExpiryExtensions
    {
        #region Expiry Extensions
        public static async Task<RedisStorage<T>> Expire<T>(this RedisStorage<T> storage, TimeSpan expiry) where T : class, new()
        {
            return await storage.ExpireAsync(expiry);
        }

        public static async Task<RedisStorage<T>> ExpireAt<T>(this RedisStorage<T> storage, DateTime expiryAt) where T : class, new()
        {
            return await storage.ExpireAtAsync(expiryAt);
        }

        public static async Task<TimeSpan?> TTL<T>(this RedisStorage<T> storage) where T : class, new()
        {
            return await storage.GetTimeToLiveAsync();
        }

        public static async Task<RedisStorage<T>> Persist<T>(this RedisStorage<T> storage) where T : class, new()
        {
            return await storage.PersistAsync();
        }

        public static async Task<bool> IsExpiring<T>(this RedisStorage<T> storage) where T : class, new()
        {
            return await storage.HasExpiryAsync();
        }

        // Expiry config shortcuts
        public static RedisStorage<T> Expiry<T>(this RedisStorage<T> storage, TimeSpan expiry, bool sliding = false) where T : class, new()
        {
            storage.Config.DefaultExpiry = expiry;
            storage.Config.SlidingExpiration = sliding;
            return storage;
        }

        public static RedisStorage<T> ShortLived<T>(this RedisStorage<T> storage) where T : class, new()
        {
            storage.Config = RedisConfig.ShortLived;
            return storage;
        }

        public static RedisStorage<T> Session<T>(this RedisStorage<T> storage) where T : class, new()
        {
            storage.Config = RedisConfig.Session;
            return storage;
        }

        public static RedisStorage<T> Cache<T>(this RedisStorage<T> storage) where T : class, new()
        {
            storage.Config = RedisConfig.Cache;
            return storage;
        }

        public static RedisStorage<T> Permanent<T>(this RedisStorage<T> storage) where T : class, new()
        {
            storage.Config = RedisConfig.Permanent;
            return storage;
        }

        // Individual storage expiry methods
        public static async Task<RedisStorage<T>> ExpireKey<T>(this RedisStorage<T> storage, string itemId, TimeSpan expiry) where T : class, new()
        {
            if (storage is RedisIndividualStorage<T> individualStorage)
            {
                await individualStorage.ExpireAsync(expiry); // This expires the prefix, need individual key method
                                                             // For individual keys, we need direct database access
                var db = GetDatabase(storage);
                var fullKey = $"{storage.GetKey()}:{itemId}";
                await db.KeyExpireAsync(fullKey, expiry);
                return storage;
            }
            throw new NotSupportedException("ExpireKey only supported for Individual storage");
        }

        public static async Task<TimeSpan?> TTLKey<T>(this RedisStorage<T> storage, string itemId) where T : class, new()
        {
            if (storage is RedisIndividualStorage<T> individualStorage)
            {
                var db = GetDatabase(storage);
                var fullKey = $"{storage.GetKey()}:{itemId}";
                return await db.KeyTimeToLiveAsync(fullKey);
            }
            throw new NotSupportedException("TTLKey only supported for Individual storage");
        }

        private static IDatabase GetDatabase<T>(RedisStorage<T> storage) where T : class, new()
        {
            var dbField = typeof(RedisStorage<T>).GetField("_db",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            return dbField?.GetValue(storage) as IDatabase;
        }

        private static string GetKey<T>(this RedisStorage<T> storage) where T : class, new()
        {
            var keyField = typeof(RedisStorage<T>).GetField("_key",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            return keyField?.GetValue(storage) as string;
        }
        #endregion
    }
}
