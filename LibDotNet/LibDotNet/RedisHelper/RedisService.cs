using Libs.Helpers;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace Libs.RedisHelper
{
    public class RedisService
    {
        private readonly IConnectionMultiplexer _redis;
        private readonly IDatabase _db;

        public RedisService(IConnectionMultiplexer redis)
        {
            _redis = redis;
            _db = redis.GetDatabase();
        }

        // Storage methods với config
        public RedisStorage<T> Hash<T>(string key, RedisOperationConfig config = null) where T : class, new()
            => new RedisHashStorage<T>(_db, key) { Config = config ?? RedisConfig.Optimized };

        public RedisStorage<T> List<T>(string key, RedisOperationConfig config = null) where T : class, new()
            => new RedisListStorage<T>(_db, key) { Config = config ?? RedisConfig.Optimized };

        public RedisStorage<T> String<T>(string key, RedisOperationConfig config = null) where T : class, new()
            => new RedisStringStorage<T>(_db, key) { Config = config ?? RedisConfig.Optimized };

        public RedisStorage<T> Individual<T>(string key, RedisOperationConfig config = null) where T : class, new()
            => new RedisIndividualStorage<T>(_db, _redis, key) { Config = config ?? RedisConfig.Optimized };

        // Shortcut methods
        public RedisStorage<T> H<T>(string key, RedisOperationConfig config = null) where T : class, new()
            => Hash<T>(key, config);

        public RedisStorage<T> L<T>(string key, RedisOperationConfig config = null) where T : class, new()
            => List<T>(key, config);

        public RedisStorage<T> S<T>(string key, RedisOperationConfig config = null) where T : class, new()
            => String<T>(key, config);

        public RedisStorage<T> I<T>(string key, RedisOperationConfig config = null) where T : class, new()
            => Individual<T>(key, config);

        // Utility methods
        public async Task<bool> Exists(string key) => await _db.KeyExistsAsync(key);
        public async Task<bool> Delete(string key) => await _db.KeyDeleteAsync(key);
        public async Task<bool> Expire(string key, TimeSpan expiry) => await _db.KeyExpireAsync(key, expiry);
        public async Task<TimeSpan> Ttl(string key) => await _db.KeyTimeToLiveAsync(key) ?? TimeSpan.Zero;
    }
}