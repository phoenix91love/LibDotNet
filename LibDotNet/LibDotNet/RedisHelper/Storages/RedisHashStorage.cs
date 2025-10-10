using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Libs.RedisHelper
{

    public class RedisHashStorage<T> : RedisStorage<T> where T : class, new()
    {
        public RedisHashStorage(IDatabase db, string key) : base(db, key) { }

        // INSERT implementations
        protected override Task InsertInPipeline(IBatch batch, T item, string itemId)
        {
            ApplyTracking(item);
            var id = itemId ?? GetItemId(item);
            return batch.HashSetAsync(_key, id, Serialize(item));
        }

        protected override Task InsertAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId)
        {
            var itemList = items.ToList();
            var entries = itemList.Select(item =>
            {
                ApplyTracking(item);
                var id = getId?.Invoke(item) ?? GetItemId(item);
                return new HashEntry(id, Serialize(item));
            }).ToArray();

            return entries.Length > 0 ? batch.HashSetAsync(_key, entries) : Task.CompletedTask;
        }

        // UPDATE implementations
        protected override Task UpdateInPipeline(IBatch batch, T item, string itemId)
        {
            ApplyTracking(item);
            var id = itemId ?? GetItemId(item);
            return batch.HashSetAsync(_key, id, Serialize(item));
        }

        protected override Task UpdateAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId)
        {
            var itemList = items.ToList();
            var entries = itemList.Select(item =>
            {
                ApplyTracking(item);
                var id = getId?.Invoke(item) ?? GetItemId(item);
                return new HashEntry(id, Serialize(item));
            }).ToArray();

            return entries.Length > 0 ? batch.HashSetAsync(_key, entries) : Task.CompletedTask;
        }

        // DELETE implementations
        protected override Task DeleteInPipeline(IBatch batch, string itemId)
        {
            return batch.HashDeleteAsync(_key, itemId);
        }

        protected override Task DeleteAllInPipeline(IBatch batch, IEnumerable<string> itemIds)
        {
            var redisValues = itemIds.Select(id => (RedisValue)id).ToArray();
            return redisValues.Length > 0 ? batch.HashDeleteAsync(_key, redisValues) : Task.CompletedTask;
        }

        // GET implementations
        public override async Task<T> GetAsync(string itemId)
        {
            await ExecutePipelineIfNeeded();
            var value = await _db.HashGetAsync(_key, itemId);
            return value.IsNullOrEmpty ? null : Deserialize(value);
        }

        public override async Task<List<T>> GetAllAsync()
        {
            await ExecutePipelineIfNeeded();
            var entries = await _db.HashGetAllAsync(_key);
            return entries.Where(e => !e.Value.IsNullOrEmpty)
                         .Select(e => Deserialize(e.Value))
                         .Where(item => item != null)
                         .ToList();
        }

        public override async Task<bool> ExistsAsync(string itemId)
        {
            await ExecutePipelineIfNeeded();
            return await _db.HashExistsAsync(_key, itemId);
        }

        public override async Task<long> CountAsync()
        {
            await ExecutePipelineIfNeeded();
            return await _db.HashLengthAsync(_key);
        }

        // Additional methods
        public async Task<List<string>> GetKeysAsync()
        {
            await ExecutePipelineIfNeeded();
            var keys = await _db.HashKeysAsync(_key);
            return keys.Select(k => k.ToString()).ToList();
        }

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

        public async Task<RedisStorage<T>> IncrementAsync(string itemId, string propertyName, long increment = 1)
        {
            var currentItem = await GetAsync(itemId);
            if (currentItem != null)
            {
                var property = typeof(T).GetProperty(propertyName);
                if (property != null)
                {
                    var currentValue = Convert.ToInt64(property.GetValue(currentItem));
                    property.SetValue(currentItem, currentValue + increment);
                    ApplyTracking(currentItem);
                    await UpdateAsync(currentItem, itemId);
                }
            }
            return this;
        }

        public async Task<RedisStorage<T>> UpdateIfAsync(string itemId, Func<T, bool> condition, Action<T> updateAction)
        {
            var currentItem = await GetAsync(itemId);
            if (currentItem != null && condition(currentItem))
            {
                updateAction(currentItem);
                ApplyTracking(currentItem);
                await UpdateAsync(currentItem, itemId);
            }
            return this;
        }
    }
}
