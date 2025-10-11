using Libs.Helpers;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Libs.RedisHelper
{
    public class RedisListStorage<T> : RedisStorage<T> where T : class, new()
    {
        public RedisListStorage(IDatabase db, string key) : base(db, key) { }

        #region INSERT Operations với Expiry
        protected override Task InsertInPipeline(IBatch batch, T item, string itemId)
        {
            ApplyTracking(item);
            ApplyExpiryTracking(item);
            var task = batch.ListRightPushAsync(_key, Serialize(item));

            // ✅ Apply expiry cho list
            if (Config.DefaultExpiry.HasValue)
            {
                return Task.WhenAll(task, batch.KeyExpireAsync(_key, Config.DefaultExpiry.Value));
            }

            return task;
        }

        protected override Task InsertAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId)
        {
            var itemList = items.ToList();
            var values = itemList.Select(item =>
            {
                ApplyTracking(item);
                ApplyExpiryTracking(item);
                return (RedisValue)Serialize(item);
            }).ToArray();

            var task = values.Length > 0 ? batch.ListRightPushAsync(_key, values) : Task.CompletedTask;

            // ✅ Apply expiry cho entire list
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
        public async Task<RedisStorage<T>> InsertAtAsync(int index, T item)
        {
            // For list, we need to handle insert at specific position
            var currentItems = await GetAllAsync();
            if (index >= 0 && index <= currentItems.Count)
            {
                currentItems.Insert(index, item);
                await ClearAsync();
                await InsertAllAsync(currentItems);
            }
            return this;
        }
        #endregion

        #region UPDATE Operations với Expiry
        protected override Task UpdateInPipeline(IBatch batch, T item, string itemId)
        {
            // For list storage, update is handled via index replacement
            ApplyTracking(item);
            ApplyExpiryTracking(item);

            // This will be handled in the UpdateAsync method
            return Task.CompletedTask;
        }

        protected override Task UpdateAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId)
        {
            // For list, we replace the entire list, so we handle expiry there
            return Task.CompletedTask;
        }

        public override async Task<RedisStorage<T>> UpdateAsync(T item, string itemId = null)
        {
            // For list storage, we need to find and replace the item
            var currentItems = await GetAllAsync();
            var index = currentItems.FindIndex(i => GetItemId(i) == GetItemId(item));

            if (index >= 0)
            {
                AddToPipeline(batch =>
                {
                    ApplyTracking(item);
                    ApplyExpiryTracking(item);
                    return batch.ListSetByIndexAsync(_key, index, Serialize(item));
                });

                // ✅ Apply expiry on update
                if (Config.DefaultExpiry.HasValue)
                {
                    AddToPipeline(batch => batch.KeyExpireAsync(_key, Config.DefaultExpiry.Value));
                }

                await ExecutePipelineIfNeeded();
            }
            return this;
        }

        public override async Task<RedisStorage<T>> UpdateAllAsync(IEnumerable<T> items, Func<T, string> getId = null)
        {
            // For list, we replace the entire list
            await ClearAsync();
            await InsertAllAsync(items, getId);
            return this;
        }
        public async Task<RedisStorage<T>> UpdateAtAsync(int index, T item)
        {
            if (index >= 0)
            {
                AddToPipeline(batch => batch.ListSetByIndexAsync(_key, index, Serialize(item)));
                await ExecutePipelineIfNeeded();
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

            var allItems = await GetAllAsync();
            return allItems.FirstOrDefault(item => GetItemId(item) == itemId);
        }

        public override async Task<List<T>> GetAllAsync()
        {
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry trên GET operations
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            var values = await _db.ListRangeAsync(_key);
            return values.Where(v => !v.IsNullOrEmpty)
                        .Select(v => Deserialize(v))
                        .Where(item => item != null)
                        .ToList();
        }

        public async Task<T> GetAtAsync(int index)
        {
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            var value = await _db.ListGetByIndexAsync(_key, index);
            return value.IsNullOrEmpty ? null : Deserialize(value);
        }

        public async Task<List<T>> GetRangeAsync(int start, int stop)
        {
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            var values = await _db.ListRangeAsync(_key, start, stop);
            return values.Where(v => !v.IsNullOrEmpty)
                        .Select(v => Deserialize(v))
                        .Where(item => item != null)
                        .ToList();
        }
        #endregion

        #region DELETE Operations với Expiry Maintenance
        protected override Task DeleteInPipeline(IBatch batch, string itemId)
        {
            // This is handled in DeleteAsync which finds and removes the specific item
            return Task.CompletedTask;
        }

        protected override Task DeleteAllInPipeline(IBatch batch, IEnumerable<string> itemIds)
        {
            // This is handled in DeleteAllAsync
            return Task.CompletedTask;
        }

        public override async Task<RedisStorage<T>> DeleteAsync(string itemId)
        {
            var currentItems = await GetAllAsync();
            var item = currentItems.FirstOrDefault(i => GetItemId(i) == itemId);

            if (item != null)
            {
                var json = Serialize(item);
                AddToPipeline(batch => batch.ListRemoveAsync(_key, json));

                // ✅ Maintain expiry on delete operations
                if (Config.DefaultExpiry.HasValue)
                {
                    AddToPipeline(batch => batch.KeyExpireAsync(_key, Config.DefaultExpiry.Value));
                }

                await ExecutePipelineIfNeeded();
            }
            return this;
        }

        public override async Task<RedisStorage<T>> DeleteAllAsync(IEnumerable<string> itemIds)
        {
            var currentItems = await GetAllAsync();
            var itemsToRemove = currentItems.Where(i => itemIds.Contains(GetItemId(i))).ToList();

            foreach (var item in itemsToRemove)
            {
                var json = Serialize(item);
                AddToPipeline(batch => batch.ListRemoveAsync(_key, json));
            }

            // ✅ Maintain expiry
            if (Config.DefaultExpiry.HasValue && itemsToRemove.Count > 0)
            {
                AddToPipeline(batch => batch.KeyExpireAsync(_key, Config.DefaultExpiry.Value));
            }

            await ExecutePipelineIfNeeded();
            return this;
        }
        public async Task<RedisStorage<T>> DeleteAtAsync(int index)
        {
            // For list, we set the value to null and then remove null values
            await UpdateAtAsync(index, null);
            AddToPipeline(batch => batch.ListRemoveAsync(_key, RedisValue.Null));
            await ExecutePipelineIfNeeded();
            return this;
        }

        public async Task<RedisStorage<T>> DeleteRangeAsync(int start, int stop)
        {
            // Redis doesn't have direct range delete, so we use trim
            var length = await CountAsync();
            if (start == 0 && stop == -1)
            {
                await ClearAsync();
            }
            else
            {
                // For complex range delete, we need to rebuild the list
                var items = await GetAllAsync();
                var newItems = items.Take(start).Concat(items.Skip(stop + 1)).ToList();
                await UpdateAllAsync(newItems);
            }
            return this;
        }
        #endregion

        #region List-specific Operations với Expiry
        public async Task<RedisStorage<T>> InsertLeftAsync(T item)
        {
            AddToPipeline(batch =>
            {
                ApplyTracking(item);
                ApplyExpiryTracking(item);
                var task = batch.ListLeftPushAsync(_key, Serialize(item));

                if (Config.DefaultExpiry.HasValue)
                {
                    return Task.WhenAll(task, batch.KeyExpireAsync(_key, Config.DefaultExpiry.Value));
                }
                return task;
            });

            await ExecutePipelineIfNeeded();

            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            return this;
        }

        public async Task<T> PopLeftAsync()
        {
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            var value = await _db.ListLeftPopAsync(_key);
            return value.IsNullOrEmpty ? null : Deserialize(value);
        }

        public async Task<T> PopRightAsync()
        {
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            var value = await _db.ListRightPopAsync(_key);
            return value.IsNullOrEmpty ? null : Deserialize(value);
        }

        public async Task<RedisStorage<T>> TrimAsync(int start, int stop)
        {
            AddToPipeline(batch =>
            {
                var task = batch.ListTrimAsync(_key, start, stop);

                // ✅ Maintain expiry on trim operations
                if (Config.DefaultExpiry.HasValue)
                {
                    return Task.WhenAll(task, batch.KeyExpireAsync(_key, Config.DefaultExpiry.Value));
                }
                return task;
            });

            await ExecutePipelineIfNeeded();
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

            var allItems = await GetAllAsync();
            return allItems.Any(item => GetItemId(item) == itemId);
        }

        public override async Task<long> CountAsync()
        {
            await ExecutePipelineIfNeeded();

            // ✅ Apply sliding expiry
            if (Config.SlidingExpiration && Config.DefaultExpiry.HasValue)
            {
                await ApplySlidingExpiryAsync();
            }

            return await _db.ListLengthAsync(_key);
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
