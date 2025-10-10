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

        #region INSERT Operations
        public override async Task<RedisStorage<T>> InsertAsync(T item, string itemId = null)
        {
            AddToPipeline(batch => InsertInPipeline(batch, item, itemId));
            await ExecutePipelineIfNeeded();
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
            return this;
        }

        protected override Task InsertInPipeline(IBatch batch, T item, string itemId)
        {
            ApplyTracking(item);
            return batch.ListRightPushAsync(_key, Serialize(item));
        }

        protected override Task InsertAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId)
        {
            var itemList = items.ToList();
            var values = itemList.Select(item =>
            {
                ApplyTracking(item);
                return (RedisValue)Serialize(item);
            }).ToArray();

            return values.Length > 0 ? batch.ListRightPushAsync(_key, values) : Task.CompletedTask;
        }

        // Additional insert methods
        public async Task<RedisStorage<T>> InsertLeftAsync(T item)
        {
            AddToPipeline(batch => batch.ListLeftPushAsync(_key, Serialize(item)));
            await ExecutePipelineIfNeeded();
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

        #region UPDATE Operations
        public override async Task<RedisStorage<T>> UpdateAsync(T item, string itemId = null)
        {
            // For list storage, we need to find and replace the item
            var currentItems = await GetAllAsync();
            var index = currentItems.FindIndex(i => GetItemId(i) == GetItemId(item));

            if (index >= 0)
            {
                AddToPipeline(batch => batch.ListSetByIndexAsync(_key, index, Serialize(item)));
                await ExecutePipelineIfNeeded();
            }
            return this;
        }

        public override async Task<RedisStorage<T>> UpdateAsync(string itemId, Action<T> updateAction)
        {
            var currentItems = await GetAllAsync();
            var item = currentItems.FirstOrDefault(i => GetItemId(i) == itemId);

            if (item != null)
            {
                updateAction(item);
                await UpdateAsync(item, itemId);
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

        protected override Task UpdateInPipeline(IBatch batch, T item, string itemId)
        {
            // This is handled differently for lists - we use UpdateAsync override
            return Task.CompletedTask;
        }

        protected override Task UpdateAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId)
        {
            // This is handled differently for lists - we use UpdateAllAsync override
            return Task.CompletedTask;
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

        #region DELETE Operations
        public override async Task<RedisStorage<T>> DeleteAsync(string itemId)
        {
            var currentItems = await GetAllAsync();
            var item = currentItems.FirstOrDefault(i => GetItemId(i) == itemId);

            if (item != null)
            {
                var json = Serialize(item);
                AddToPipeline(batch => batch.ListRemoveAsync(_key, json));
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

            await ExecutePipelineIfNeeded();
            return this;
        }

        protected override Task DeleteInPipeline(IBatch batch, string itemId)
        {
            // This is handled in DeleteAsync override
            return Task.CompletedTask;
        }

        protected override Task DeleteAllInPipeline(IBatch batch, IEnumerable<string> itemIds)
        {
            // This is handled in DeleteAllAsync override
            return Task.CompletedTask;
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

        public async Task<T> PopLeftAsync()
        {
            await ExecutePipelineIfNeeded();
            var value = await _db.ListLeftPopAsync(_key);
            return value.IsNullOrEmpty ? null : Deserialize(value);
        }

        public async Task<T> PopRightAsync()
        {
            await ExecutePipelineIfNeeded();
            var value = await _db.ListRightPopAsync(_key);
            return value.IsNullOrEmpty ? null : Deserialize(value);
        }
        #endregion

        #region GET Operations
        public override async Task<T> GetAsync(string itemId)
        {
            await ExecutePipelineIfNeeded();
            var allItems = await GetAllAsync();
            return allItems.FirstOrDefault(item => GetItemId(item) == itemId);
        }

        public override async Task<List<T>> GetAllAsync()
        {
            await ExecutePipelineIfNeeded();
            var values = await _db.ListRangeAsync(_key);
            return values.Where(v => !v.IsNullOrEmpty)
                        .Select(v => Deserialize(v))
                        .Where(item => item != null)
                        .ToList();
        }

        public override async Task<bool> ExistsAsync(string itemId)
        {
            await ExecutePipelineIfNeeded();
            var allItems = await GetAllAsync();
            return allItems.Any(item => GetItemId(item) == itemId);
        }

        public override async Task<long> CountAsync()
        {
            await ExecutePipelineIfNeeded();
            return await _db.ListLengthAsync(_key);
        }

        public async Task<T> GetAtAsync(int index)
        {
            await ExecutePipelineIfNeeded();
            var value = await _db.ListGetByIndexAsync(_key, index);
            return value.IsNullOrEmpty ? null : Deserialize(value);
        }

        public async Task<List<T>> GetRangeAsync(int start, int stop)
        {
            await ExecutePipelineIfNeeded();
            var values = await _db.ListRangeAsync(_key, start, stop);
            return values.Where(v => !v.IsNullOrEmpty)
                        .Select(v => Deserialize(v))
                        .Where(item => item != null)
                        .ToList();
        }
        #endregion

        #region Utility Methods
        public async Task<RedisStorage<T>> TrimAsync(int start, int stop)
        {
            AddToPipeline(batch => batch.ListTrimAsync(_key, start, stop));
            await ExecutePipelineIfNeeded();
            return this;
        }

        public async Task<RedisStorage<T>> ClearAsync()
        {
            await ExecutePipelineIfNeeded();
            await _db.KeyDeleteAsync(_key);
            return this;
        }

        public async Task<int> IndexOfAsync(string itemId)
        {
            var allItems = await GetAllAsync();
            return allItems.FindIndex(item => GetItemId(item) == itemId);
        }

        public async Task<bool> ContainsAsync(T item)
        {
            var allItems = await GetAllAsync();
            return allItems.Any(i => GetItemId(i) == GetItemId(item));
        }
        #endregion
    }
}
