using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Libs.RedisHelper
{
    public class RedisStringStorage<T> : RedisStorage<T> where T : class, new()
    {
        public RedisStringStorage(IDatabase db, string key) : base(db, key) { }

        #region INSERT Operations với Expiry
        protected override Task InsertInPipeline(IBatch batch, T item, string itemId)
        {
            ApplyTracking(item);
            ApplyExpiryTracking(item);
            var task = batch.StringSetAsync(_key, Serialize(item));

            // ✅ Apply expiry
            if (Config.DefaultExpiry.HasValue)
            {
                return Task.WhenAll(task, batch.KeyExpireAsync(_key, Config.DefaultExpiry.Value));
            }

            return task;
        }

        protected override Task InsertAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId)
        {
            var itemList = items.ToList();
            ApplyTrackingToAll(itemList);
            foreach (var item in itemList)
            {
                ApplyExpiryTracking(item);
            }

            var json = JsonSerializer.Serialize(itemList, _jsonOptions);
            var task = batch.StringSetAsync(_key, json);

            // ✅ Apply expiry
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

            // ✅ Apply sliding expiry
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            return this;
        }

        public override async Task<RedisStorage<T>> InsertAllAsync(IEnumerable<T> items, Func<T, string> getId = null)
        {
            var itemList = items.ToList();
            AddToPipeline(batch => InsertAllInPipeline(batch, itemList, getId));
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
            var task = batch.StringSetAsync(_key, Serialize(item));

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
            ApplyTrackingToAll(itemList);
            foreach (var item in itemList)
            {
                ApplyExpiryTracking(item);
            }

            var json = JsonSerializer.Serialize(itemList, _jsonOptions);
            var task = batch.StringSetAsync(_key, json);

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

        public override async Task<RedisStorage<T>> UpdateAsync(string itemId, Action<T> updateAction)
        {
            var currentItem = await GetAsync();
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
            AddToPipeline(batch => UpdateAllInPipeline(batch, itemList, getId));
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            return this;
        }

        // Special method for updating when storing collections
        public async Task<RedisStorage<T>> UpdateInCollectionAsync(string itemId, Action<T> updateAction)
        {
            var allItems = await GetAllAsync();
            var itemToUpdate = allItems.FirstOrDefault(item => GetItemId(item) == itemId);

            if (itemToUpdate != null)
            {
                updateAction(itemToUpdate);
                ApplyTracking(itemToUpdate);
                ApplyExpiryTracking(itemToUpdate);

                // Update the entire collection
                var json = JsonSerializer.Serialize(allItems, _jsonOptions);
                await _db.StringSetAsync(_key, json);

                // ✅ Apply expiry
                if (Config.DefaultExpiry.HasValue)
                {
                    await _db.KeyExpireAsync(_key, Config.DefaultExpiry.Value);
                }
            }
            return this;
        }
        #endregion

        #region GET Operations với Sliding Expiry
        public override async Task<T> GetAsync(string itemId = null)
        {
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry trên GET operations
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            var value = await _db.StringGetAsync(_key);

            if (value.IsNullOrEmpty)
                return null;

            // Try to deserialize as single object first
            try
            {
                return Deserialize(value);
            }
            catch
            {
                // If it fails, it might be a collection - return first item or null
                try
                {
                    var collection = JsonSerializer.Deserialize<List<T>>(value, _jsonOptions);
                    return collection?.FirstOrDefault();
                }
                catch
                {
                    return null;
                }
            }
        }

        public override async Task<List<T>> GetAllAsync()
        {
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry trên GET operations
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            var value = await _db.StringGetAsync(_key);

            if (value.IsNullOrEmpty)
                return new List<T>();

            // Try to deserialize as collection first
            try
            {
                return JsonSerializer.Deserialize<List<T>>(value, _jsonOptions) ?? new List<T>();
            }
            catch
            {
                // If it fails, it might be a single object - wrap in list
                try
                {
                    var singleItem = Deserialize(value);
                    return singleItem != null ? new List<T> { singleItem } : new List<T>();
                }
                catch
                {
                    return new List<T>();
                }
            }
        }

        // Get specific item from collection
        public async Task<T> GetFromCollectionAsync(string itemId)
        {
            var allItems = await GetAllAsync();
            return allItems.FirstOrDefault(item => GetItemId(item) == itemId);
        }
        #endregion

        #region DELETE Operations với Expiry
        protected override Task DeleteInPipeline(IBatch batch, string itemId)
        {
            return batch.KeyDeleteAsync(_key);
        }

        protected override Task DeleteAllInPipeline(IBatch batch, IEnumerable<string> itemIds)
        {
            // This is handled in DeleteAllAsync
            return Task.CompletedTask;
        }

        public override async Task<RedisStorage<T>> DeleteAsync(string itemId)
        {
            AddToPipeline(batch => DeleteInPipeline(batch, itemId));
            await ExecutePipelineIfNeeded();
            return this;
        }

        public override async Task<RedisStorage<T>> DeleteAllAsync(IEnumerable<string> itemIds)
        {
            // For string storage storing collection, remove items from collection
            var allItems = await GetAllAsync();
            var updatedItems = allItems.Where(item => !itemIds.Contains(GetItemId(item))).ToList();

            if (updatedItems.Count != allItems.Count)
            {
                var json = JsonSerializer.Serialize(updatedItems, _jsonOptions);
                await _db.StringSetAsync(_key, json);

                // ✅ Maintain expiry
                if (Config.DefaultExpiry.HasValue)
                {
                    await _db.KeyExpireAsync(_key, Config.DefaultExpiry.Value);
                }
            }
            return this;
        }

        // Delete item from collection
        public async Task<RedisStorage<T>> DeleteFromCollectionAsync(string itemId)
        {
            var allItems = await GetAllAsync();
            var updatedItems = allItems.Where(item => GetItemId(item) != itemId).ToList();

            var json = JsonSerializer.Serialize(updatedItems, _jsonOptions);
            await _db.StringSetAsync(_key, json);

            // ✅ Maintain expiry
            if (Config.DefaultExpiry.HasValue)
            {
                await _db.KeyExpireAsync(_key, Config.DefaultExpiry.Value);
            }

            return this;
        }
        #endregion

        #region Utility Methods với Expiry
        private void ApplyTrackingToAll(IEnumerable<T> items)
        {
            if (Config.EnableTracking)
            {
                var timestampProperty = typeof(T).GetProperty(Config.TimestampProperty);
                if (timestampProperty != null)
                {
                    foreach (var item in items)
                    {
                        timestampProperty.SetValue(item, DateTime.Now);
                    }
                }
            }
        }

        public async Task<RedisStorage<T>> AppendAsync(T item)
        {
            var allItems = await GetAllAsync();
            allItems.Add(item);
            var json = JsonSerializer.Serialize(allItems, _jsonOptions);
            await _db.StringSetAsync(_key, json);

            // ✅ Maintain expiry
            if (Config.DefaultExpiry.HasValue)
            {
                await _db.KeyExpireAsync(_key, Config.DefaultExpiry.Value);
            }

            return this;
        }

        public override async Task<bool> ExistsAsync(string itemId = null)
        {
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            return await _db.KeyExistsAsync(_key);
        }

        public override async Task<long> CountAsync()
        {
            var allItems = await GetAllAsync();
            return allItems.Count;
        }

        public override async Task<RedisStorage<T>> ClearAsync()
        {
            await ExecutePipelineIfNeeded();
            await _db.KeyDeleteAsync(_key);
            return this;
        }
        #endregion
    }
}
