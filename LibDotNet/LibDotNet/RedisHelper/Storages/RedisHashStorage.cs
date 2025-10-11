using Libs.Helpers;
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

        #region INSERT Operations với Expiry
        protected override Task InsertInPipeline(IBatch batch, T item, string itemId)
        {
            ApplyTracking(item);
            ApplyExpiryTracking(item);
            var id = itemId ?? GetItemId(item);
            var task = batch.HashSetAsync(_key, id, Serialize(item));

            // ✅ Apply expiry cho hash
            if (Config.DefaultExpiry.HasValue)
            {
                return Task.WhenAll(task, batch.KeyExpireAsync(_key, Config.DefaultExpiry.Value));
            }

            return task;
        }

        protected override Task InsertAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId)
        {
            var itemList = items.ToList();
            var entries = itemList.Select(item =>
            {
                ApplyTracking(item);
                ApplyExpiryTracking(item);
                var id = getId?.Invoke(item) ?? GetItemId(item);
                return new HashEntry(id, Serialize(item));
            }).ToArray();

            var task = entries.Length > 0 ? batch.HashSetAsync(_key, entries) : Task.CompletedTask;

            // ✅ Apply expiry cho entire hash
            if (Config.DefaultExpiry.HasValue)
            {
                return Task.WhenAll(task, batch.KeyExpireAsync(_key, Config.DefaultExpiry.Value));
            }

            return task;
        }

        public override async Task<RedisStorage<T>> InsertAsync(T item, string itemId = null)
        {
            AddToPipeline(batch => InsertInPipeline(batch, item, itemId));
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry trên INSERT operations
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            return this;
        }

        public override async Task<RedisStorage<T>> InsertAllAsync(IEnumerable<T> items, Func<T, string> getId = null)
        {
            var itemList = items.ToList();

            foreach (var chunk in itemList.Chunk(Config.BatchSize))
            {
                AddToPipeline(batch => InsertAllInPipeline(batch, chunk, getId));
            }

            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            return this;
        }
        #endregion

        #region UPDATE Operations với Expiry
        protected override Task UpdateInPipeline(IBatch batch, T item, string itemId)
        {
            ApplyTracking(item);
            ApplyExpiryTracking(item);
            var id = itemId ?? GetItemId(item);
            var task = batch.HashSetAsync(_key, id, Serialize(item));

            // ✅ Apply expiry
            if (Config.DefaultExpiry.HasValue)
            {
                return Task.WhenAll(task, batch.KeyExpireAsync(_key, Config.DefaultExpiry.Value));
            }

            return task;
        }

        protected override Task UpdateAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId)
        {
            var itemList = items.ToList();
            var entries = itemList.Select(item =>
            {
                ApplyTracking(item);
                ApplyExpiryTracking(item);
                var id = getId?.Invoke(item) ?? GetItemId(item);
                return new HashEntry(id, Serialize(item));
            }).ToArray();

            var task = entries.Length > 0 ? batch.HashSetAsync(_key, entries) : Task.CompletedTask;

            // ✅ Apply expiry
            if (Config.DefaultExpiry.HasValue)
            {
                return Task.WhenAll(task, batch.KeyExpireAsync(_key, Config.DefaultExpiry.Value));
            }

            return task;
        }

        public override async Task<RedisStorage<T>> UpdateAsync(T item, string itemId = null)
        {
            AddToPipeline(batch => UpdateInPipeline(batch, item, itemId));
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            return this;
        }

        public override async Task<RedisStorage<T>> UpdateAllAsync(IEnumerable<T> items, Func<T, string> getId = null)
        {
            var itemList = items.ToList();

            foreach (var chunk in itemList.Chunk(Config.BatchSize))
            {
                AddToPipeline(batch => UpdateAllInPipeline(batch, chunk, getId));
            }

            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            return this;
        }
        #endregion

        #region GET Operations với Sliding Expiry
        public override async Task<T> GetAsync(string itemId)
        {
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry trên GET operations
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            var value = await _db.HashGetAsync(_key, itemId);
            return value.IsNullOrEmpty ? null : Deserialize(value);
        }

        public override async Task<List<T>> GetAllAsync()
        {
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry trên GET operations
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            var entries = await _db.HashGetAllAsync(_key);
            return entries.Where(e => !e.Value.IsNullOrEmpty)
                         .Select(e => Deserialize(e.Value))
                         .Where(item => item != null)
                         .ToList();
        }
        #endregion

        #region DELETE Operations với Expiry Maintenance
        protected override Task DeleteInPipeline(IBatch batch, string itemId)
        {
            var task = batch.HashDeleteAsync(_key, itemId);

            // ✅ Maintain expiry on delete operations
            if (Config.DefaultExpiry.HasValue)
            {
                return Task.WhenAll(task, batch.KeyExpireAsync(_key, Config.DefaultExpiry.Value));
            }

            return task;
        }

        protected override Task DeleteAllInPipeline(IBatch batch, IEnumerable<string> itemIds)
        {
            var redisValues = itemIds.Select(id => (RedisValue)id).ToArray();
            var task = redisValues.Length > 0 ? batch.HashDeleteAsync(_key, redisValues) : Task.CompletedTask;

            // ✅ Maintain expiry
            if (Config.DefaultExpiry.HasValue && redisValues.Length > 0)
            {
                return Task.WhenAll(task, batch.KeyExpireAsync(_key, Config.DefaultExpiry.Value));
            }

            return task;
        }

        public override async Task<RedisStorage<T>> DeleteAsync(string itemId)
        {
            AddToPipeline(batch => DeleteInPipeline(batch, itemId));
            await ExecutePipelineIfNeeded();
            return this;
        }

        public override async Task<RedisStorage<T>> DeleteAllAsync(IEnumerable<string> itemIds)
        {
            foreach (var chunk in itemIds.Chunk(Config.BatchSize))
            {
                AddToPipeline(batch => DeleteAllInPipeline(batch, chunk));
            }

            await ExecutePipelineIfNeeded();
            return this;
        }
        #endregion

        #region Hash-specific Operations với Expiry
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
                    ApplyExpiryTracking(currentItem);
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
                ApplyExpiryTracking(currentItem);
                await UpdateAsync(currentItem, itemId);
            }
            return this;
        }
        #endregion

        #region Utility Methods
        public override async Task<bool> ExistsAsync(string itemId)
        {
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            return await _db.HashExistsAsync(_key, itemId);
        }

        public override async Task<long> CountAsync()
        {
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            return await _db.HashLengthAsync(_key);
        }

        public override async Task<RedisStorage<T>> ClearAsync()
        {
            await ExecutePipelineIfNeeded();
            await _db.KeyDeleteAsync(_key);
            return this;
        }

        public async Task<List<string>> GetKeysAsync()
        {
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            var keys = await _db.HashKeysAsync(_key);
            return keys.Select(k => k.ToString()).ToList();
        }
        #endregion
    }
}
