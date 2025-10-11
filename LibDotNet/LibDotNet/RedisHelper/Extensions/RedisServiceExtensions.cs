using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Libs.RedisHelper
{
    public static class RedisServiceExtensions
    {
        // INSERT shortcuts
        public static async Task<RedisStorage<T>> Insert<T>(this RedisStorage<T> storage, T item, string itemId = null) where T : class, new()
        {
            return await storage.InsertAsync(item, itemId);
        }

        public static async Task<RedisStorage<T>> Insert<T>(this RedisStorage<T> storage, IEnumerable<T> items, Func<T, string> getId = null) where T : class, new()
        {
            return await storage.InsertAllAsync(items, getId);
        }

        // UPDATE shortcuts
        public static async Task<RedisStorage<T>> Update<T>(this RedisStorage<T> storage, T item, string itemId = null) where T : class, new()
        {
            return await storage.UpdateAsync(item, itemId);
        }

        public static async Task<RedisStorage<T>> Update<T>(this RedisStorage<T> storage, string itemId, Action<T> updateAction) where T : class, new()
        {
            return await storage.UpdateAsync(itemId, updateAction);
        }

        public static async Task<RedisStorage<T>> UpdateAll<T>(this RedisStorage<T> storage, IEnumerable<T> items, Func<T, string> getId = null) where T : class, new()
        {
            return await storage.UpdateAllAsync(items, getId);
        }

        public static async Task<RedisStorage<T>> UpdateProperties<T>(this RedisStorage<T> storage, string itemId, string property, object value) where T : class, new()
        {
            return await storage.UpdatePropertyAsync(itemId, property, value);
        }

        // DELETE shortcuts
        public static async Task<RedisStorage<T>> Delete<T>(this RedisStorage<T> storage, string itemId) where T : class, new()
        {
            return await storage.DeleteAsync(itemId);
        }

        public static async Task<RedisStorage<T>> DeleteAll<T>(this RedisStorage<T> storage, IEnumerable<string> itemIds) where T : class, new()
        {
            return await storage.DeleteAllAsync(itemIds);
        }

        public static async Task<RedisStorage<T>> Clear<T>(this RedisStorage<T> storage) where T : class, new()
        {
            return await storage.ClearAsync();
        }

        // GET shortcuts
        public static async Task<T> Get<T>(this RedisStorage<T> storage, string itemId) where T : class, new()
        {
            return await storage.GetAsync(itemId);
        }

        public static async Task<List<T>> GetAll<T>(this RedisStorage<T> storage) where T : class, new()
        {
            return await storage.GetAllAsync();
        }

        public static async Task<bool> Exist<T>(this RedisStorage<T> storage, string itemId) where T : class, new()
        {
            return await storage.ExistsAsync(itemId);
        }

        public static async Task<long> Count<T>(this RedisStorage<T> storage) where T : class, new()
        {
            return await storage.CountAsync();
        }

        // QUERY shortcuts
        public static async Task<List<T>> Where<T>(this RedisStorage<T> storage, Func<T, bool> filter) where T : class, new()
        {
            return await storage.WhereAsync(filter);
        }

        public static async Task<T> First<T>(this RedisStorage<T> storage, Func<T, bool> filter = null) where T : class, new()
        {
            return await storage.FirstOrDefaultAsync(filter);
        }

        // PIPELINE shortcuts
        public static RedisStorage<T> Pipe<T>(this RedisStorage<T> storage) where T : class, new()
        {
            return storage.BeginPipeline();
        }

        public static async Task<RedisStorage<T>> Run<T>(this RedisStorage<T> storage) where T : class, new()
        {
            return await storage.ExecutePipelineAsync();
        }

        public static RedisStorage<T> Cancel<T>(this RedisStorage<T> storage) where T : class, new()
        {
            return storage.DiscardPipeline();
        }

        // CONFIG shortcuts
        public static RedisStorage<T> Tran<T>(this RedisStorage<T> storage) where T : class, new()
        {
            storage.Config.UseTransaction = true;
            storage.Config.UseBatch = false;
            return storage;
        }

        public static RedisStorage<T> Batch<T>(this RedisStorage<T> storage, int size = 100) where T : class, new()
        {
            storage.Config.UseTransaction = false;
            storage.Config.UseBatch = true;
            storage.Config.BatchSize = size;
            return storage;
        }

        public static RedisStorage<T> HighPerf<T>(this RedisStorage<T> storage) where T : class, new()
        {
            storage.Config = RedisConfig.HighPerformance;
            return storage;
        }

        public static RedisStorage<T> Track<T>(this RedisStorage<T> storage) where T : class, new()
        {
            storage.Config.EnableTracking = true;
            return storage;
        }

        // SPECIAL operations (Hash storage only)
        public static async Task<RedisStorage<T>> UpdateOrInsert<T>(this RedisStorage<T> storage, T item, string itemId = null) where T : class, new()
        {
            if (storage is RedisHashStorage<T> hashStorage)
            {
                return await hashStorage.UpsertAsync(item, itemId);
            }
            throw new NotSupportedException("Upsert only supported for Hash storage");
        }

        public static async Task<RedisStorage<T>> Increment<T>(this RedisStorage<T> storage, string itemId, string property, long value = 1) where T : class, new()
        {
            if (storage is RedisHashStorage<T> hashStorage)
            {
                return await hashStorage.IncrementAsync(itemId, property, value);
            }
            throw new NotSupportedException("Increment only supported for Hash storage");
        }

        public static async Task<RedisStorage<T>> UpdateIf<T>(this RedisStorage<T> storage, string itemId, Func<T, bool> condition, Action<T> updateAction) where T : class, new()
        {
            if (storage is RedisHashStorage<T> hashStorage)
            {
                return await hashStorage.UpdateIfAsync(itemId, condition, updateAction);
            }
            throw new NotSupportedException("Conditional update only supported for Hash storage");
        }
    }

}
