using Libs.Helpers;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Libs.RedisHelper
{
    public class RedisIndividualStorage<T> : RedisStorage<T> where T : class, new()
    {
        private readonly IConnectionMultiplexer _redis;

        public RedisIndividualStorage(IDatabase db, IConnectionMultiplexer redis, string keyPrefix)
            : base(db, keyPrefix)
        {
            _redis = redis;
        }

        #region INSERT Operations
        public override async Task<RedisStorage<T>> InsertAsync(T item, string itemId = null)
        {
            var id = itemId ?? GetItemId(item);

            AddToPipeline(batch => InsertInPipeline(batch, item, id));
            await ExecutePipelineIfNeeded();
            return this;
        }

        public override async Task<RedisStorage<T>> InsertAllAsync(IEnumerable<T> items, Func<T, string> getId = null)
        {
            var itemList = items.ToList();

            // Sử dụng extension method Chunk
            var chunks = itemList.Chunk(Config.BatchSize);
            foreach (var chunk in chunks)
            {
                AddToPipeline(batch => InsertAllInPipeline(batch, chunk, getId));
            }

            await ExecutePipelineIfNeeded();
            return this;
        }

        protected override Task InsertInPipeline(IBatch batch, T item, string itemId)
        {
            ApplyTracking(item);
            var fullKey = GetFullKey(itemId);
            return batch.StringSetAsync(fullKey, Serialize(item));
        }

        protected override Task InsertAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId)
        {
            var itemList = items.ToList();
            var tasks = new List<Task>();

            foreach (var item in itemList)
            {
                ApplyTracking(item);
                var id = getId?.Invoke(item) ?? GetItemId(item);
                var fullKey = GetFullKey(id);
                tasks.Add(batch.StringSetAsync(fullKey, Serialize(item)));
            }

            return Task.WhenAll(tasks);
        }
        #endregion

        #region UPDATE Operations
        public override async Task<RedisStorage<T>> UpdateAsync(T item, string itemId = null)
        {
            var id = itemId ?? GetItemId(item);

            AddToPipeline(batch => UpdateInPipeline(batch, item, id));
            await ExecutePipelineIfNeeded();
            return this;
        }

        public override async Task<RedisStorage<T>> UpdateAsync(string itemId, Action<T> updateAction)
        {
            var currentItem = await GetAsync(itemId);
            if (currentItem != null)
            {
                updateAction(currentItem);
                await UpdateAsync(currentItem, itemId);
            }
            return this;
        }

        public override async Task<RedisStorage<T>> UpdateAllAsync(IEnumerable<T> items, Func<T, string> getId = null)
        {
            var itemList = items.ToList();

            var chunks = itemList.Chunk(Config.BatchSize);
            foreach (var chunk in chunks)
            {
                AddToPipeline(batch => UpdateAllInPipeline(batch, chunk, getId));
            }

            await ExecutePipelineIfNeeded();
            return this;
        }

        public override async Task<RedisStorage<T>> UpdatePropertyAsync(string itemId, string propertyName, object value)
        {
            var currentItem = await GetAsync(itemId);
            if (currentItem != null)
            {
                var property = typeof(T).GetProperty(propertyName);
                if (property != null && property.CanWrite)
                {
                    property.SetValue(currentItem, value);
                    ApplyTracking(currentItem);
                    await UpdateAsync(currentItem, itemId);
                }
            }
            return this;
        }

        protected override Task UpdateInPipeline(IBatch batch, T item, string itemId)
        {
            ApplyTracking(item);
            var fullKey = GetFullKey(itemId);
            return batch.StringSetAsync(fullKey, Serialize(item));
        }

        protected override Task UpdateAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId)
        {
            var itemList = items.ToList();
            var tasks = new List<Task>();

            foreach (var item in itemList)
            {
                ApplyTracking(item);
                var id = getId?.Invoke(item) ?? GetItemId(item);
                var fullKey = GetFullKey(id);
                tasks.Add(batch.StringSetAsync(fullKey, Serialize(item)));
            }

            return Task.WhenAll(tasks);
        }
        #endregion

        #region DELETE Operations
        public override async Task<RedisStorage<T>> DeleteAsync(string itemId)
        {
            AddToPipeline(batch => DeleteInPipeline(batch, itemId));
            await ExecutePipelineIfNeeded();
            return this;
        }

        public override async Task<RedisStorage<T>> DeleteAllAsync(IEnumerable<string> itemIds)
        {
            var chunks = itemIds.Chunk(Config.BatchSize);
            foreach (var chunk in chunks)
            {
                AddToPipeline(batch => DeleteAllInPipeline(batch, chunk));
            }

            await ExecutePipelineIfNeeded();
            return this;
        }

        protected override Task DeleteInPipeline(IBatch batch, string itemId)
        {
            var fullKey = GetFullKey(itemId);
            return batch.KeyDeleteAsync(fullKey);
        }

        protected override Task DeleteAllInPipeline(IBatch batch, IEnumerable<string> itemIds)
        {
            var fullKeys = itemIds.Select(id => (RedisKey)GetFullKey(id)).ToArray();
            return fullKeys.Length > 0 ? batch.KeyDeleteAsync(fullKeys) : Task.CompletedTask;
        }

        public async Task<RedisStorage<T>> ClearAllAsync()
        {
            await ExecutePipelineIfNeeded();

            var server = _redis.GetServer(_redis.GetEndPoints().First());
            var keys = server.Keys(pattern: $"{_key}:*").ToArray();

            if (keys.Length > 0)
            {
                await _db.KeyDeleteAsync(keys);
            }
            return this;
        }
        #endregion

        #region GET Operations
        public override async Task<T> GetAsync(string itemId)
        {
            await ExecutePipelineIfNeeded();
            var fullKey = GetFullKey(itemId);
            var value = await _db.StringGetAsync(fullKey);
            return value.IsNullOrEmpty ? null : Deserialize(value);
        }

        public override async Task<List<T>> GetAllAsync()
        {
            await ExecutePipelineIfNeeded();

            var server = _redis.GetServer(_redis.GetEndPoints().First());
            var keys = server.Keys(pattern: $"{_key}:*").ToArray();

            if (keys.Length == 0)
                return new List<T>();

            var values = await _db.StringGetAsync(keys);
            return values.Where(v => !v.IsNullOrEmpty)
                        .Select(v => Deserialize(v))
                        .Where(item => item != null)
                        .ToList();
        }

        public override async Task<bool> ExistsAsync(string itemId)
        {
            await ExecutePipelineIfNeeded();
            var fullKey = GetFullKey(itemId);
            return await _db.KeyExistsAsync(fullKey);
        }

        public override async Task<long> CountAsync()
        {
            await ExecutePipelineIfNeeded();

            var server = _redis.GetServer(_redis.GetEndPoints().First());
            var keys = server.Keys(pattern: $"{_key}:*").ToArray();
            return keys.Length;
        }

        // Get multiple specific items
        public async Task<List<T>> GetMultipleAsync(IEnumerable<string> itemIds)
        {
            await ExecutePipelineIfNeeded();
            var fullKeys = itemIds.Select(id => (RedisKey)GetFullKey(id)).ToArray();

            if (fullKeys.Length == 0)
                return new List<T>();

            var values = await _db.StringGetAsync(fullKeys);
            return values.Where(v => !v.IsNullOrEmpty)
                        .Select(v => Deserialize(v))
                        .Where(item => item != null)
                        .ToList();
        }

        // Get all keys
        public async Task<List<string>> GetAllKeysAsync()
        {
            await ExecutePipelineIfNeeded();

            var server = _redis.GetServer(_redis.GetEndPoints().First());
            var keys = server.Keys(pattern: $"{_key}:*").ToArray();
            return keys.Select(k => k.ToString()).ToList();
        }
        #endregion

        #region Utility Methods
        private string GetFullKey(string itemId)
        {
            return $"{_key}:{itemId}";
        }

        // Search by property value
        public async Task<List<T>> SearchByPropertyAsync(string propertyName, object value)
        {
            var allItems = await GetAllAsync();
            var property = typeof(T).GetProperty(propertyName);

            if (property == null)
                return new List<T>();

            return allItems.Where(item =>
            {
                var propValue = property.GetValue(item);
                return propValue?.Equals(value) == true;
            }).ToList();
        }

        // Bulk operations
        public async Task<RedisStorage<T>> BulkUpdatePropertiesAsync(Dictionary<string, Dictionary<string, object>> updates)
        {
            var chunks = updates.Chunk(Config.BatchSize);
            foreach (var chunk in chunks)
            {
                AddToPipeline(async batch =>
                {
                    foreach (var (itemId, properties) in chunk)
                    {
                        var currentItem = await GetAsync(itemId);
                        if (currentItem != null)
                        {
                            foreach (var prop in properties)
                            {
                                var property = typeof(T).GetProperty(prop.Key);
                                if (property != null && property.CanWrite)
                                {
                                    property.SetValue(currentItem, prop.Value);
                                }
                            }
                            ApplyTracking(currentItem);
                            await UpdateInPipeline(batch, currentItem, itemId);
                        }
                    }
                });
            }

            await ExecutePipelineIfNeeded();
            return this;
        }

        // Upsert operation
        public async Task<RedisStorage<T>> UpsertAsync(T item, string itemId = null)
        {
            var id = itemId ?? GetItemId(item);
            var exists = await ExistsAsync(id);

            if (exists)
            {
                await UpdateAsync(item, id);
            }
            else
            {
                await InsertAsync(item, id);
            }
            return this;
        }
        #endregion
    }
}
