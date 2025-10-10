using Libs.Helpers;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Libs.RedisHelper
{
    public abstract class RedisStorage<T> where T : class, new()
    {
        protected readonly IDatabase _db;
        protected readonly string _key;
        protected readonly JsonSerializerOptions _jsonOptions;
        private RedisPipelineManager _currentPipeline;

        public RedisOperationConfig Config { get; set; }

        protected RedisStorage(IDatabase db, string key)
        {
            _db = db;
            _key = key;
            _jsonOptions = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false
            };
            Config = RedisConfig.Optimized;
        }

        // Pipeline management
        protected RedisPipelineManager GetOrCreatePipeline()
        {
            if (_currentPipeline == null)
            {
                _currentPipeline = new RedisPipelineManager(_db, Config);
            }
            return _currentPipeline;
        }

        protected async Task ExecutePipelineIfNeeded()
        {
            if (_currentPipeline != null && Config.AutoExecute)
            {
                await _currentPipeline.ExecuteAsync();
                _currentPipeline = null;
            }
        }

        protected void AddToPipeline(Func<IBatch, Task> operation)
        {
            var pipeline = GetOrCreatePipeline();
            pipeline.AddOperation(operation);
        }

        // INSERT operations
        public virtual async Task<RedisStorage<T>> InsertAsync(T item, string itemId = null)
        {
            AddToPipeline(batch => InsertInPipeline(batch, item, itemId));
            await ExecutePipelineIfNeeded();
            return this;
        }

        public virtual async Task<RedisStorage<T>> InsertAllAsync(IEnumerable<T> items, Func<T, string> getId = null)
        {
            var itemList = items.ToList();

            foreach (var chunk in itemList.Chunk(Config.BatchSize))
            {
                AddToPipeline(batch => InsertAllInPipeline(batch, chunk, getId));
            }

            await ExecutePipelineIfNeeded();
            return this;
        }

        // UPDATE operations
        public virtual async Task<RedisStorage<T>> UpdateAsync(T item, string itemId = null)
        {
            AddToPipeline(batch => UpdateInPipeline(batch, item, itemId));
            await ExecutePipelineIfNeeded();
            return this;
        }

        public virtual async Task<RedisStorage<T>> UpdateAsync(string itemId, Action<T> updateAction)
        {
            var currentItem = await GetAsync(itemId);
            if (currentItem != null)
            {
                updateAction(currentItem);
                await UpdateAsync(currentItem, itemId);
            }
            return this;
        }

        public virtual async Task<RedisStorage<T>> UpdateAllAsync(IEnumerable<T> items, Func<T, string> getId = null)
        {
            var itemList = items.ToList();

            foreach (var chunk in itemList.Chunk(Config.BatchSize))
            {
                AddToPipeline(batch => UpdateAllInPipeline(batch, chunk, getId));
            }

            await ExecutePipelineIfNeeded();
            return this;
        }

        public virtual async Task<RedisStorage<T>> UpdatePropertyAsync(string itemId, string propertyName, object value)
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

        // DELETE operations
        public virtual async Task<RedisStorage<T>> DeleteAsync(string itemId)
        {
            AddToPipeline(batch => DeleteInPipeline(batch, itemId));
            await ExecutePipelineIfNeeded();
            return this;
        }

        public virtual async Task<RedisStorage<T>> DeleteAllAsync(IEnumerable<string> itemIds)
        {
            foreach (var chunk in itemIds.Chunk(Config.BatchSize))
            {
                AddToPipeline(batch => DeleteAllInPipeline(batch, chunk));
            }

            await ExecutePipelineIfNeeded();
            return this;
        }

        public virtual async Task<RedisStorage<T>> ClearAsync()
        {
            await ExecutePipelineIfNeeded();
            await _db.KeyDeleteAsync(_key);
            return this;
        }

        // GET operations
        public abstract Task<T> GetAsync(string itemId);
        public abstract Task<List<T>> GetAllAsync();
        public abstract Task<bool> ExistsAsync(string itemId);
        public abstract Task<long> CountAsync();

        // Query operations
        public virtual async Task<List<T>> WhereAsync(Func<T, bool> predicate)
        {
            var allItems = await GetAllAsync();
            return allItems.Where(predicate).ToList();
        }

        public virtual async Task<T> FirstOrDefaultAsync(Func<T, bool> predicate = null)
        {
            var allItems = await GetAllAsync();
            return predicate == null ? allItems.FirstOrDefault() : allItems.FirstOrDefault(predicate);
        }

        // Pipeline implementations (abstract - must be implemented by derived classes)
        protected abstract Task InsertInPipeline(IBatch batch, T item, string itemId);
        protected abstract Task InsertAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId);
        protected abstract Task UpdateInPipeline(IBatch batch, T item, string itemId);
        protected abstract Task UpdateAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId);
        protected abstract Task DeleteInPipeline(IBatch batch, string itemId);
        protected abstract Task DeleteAllInPipeline(IBatch batch, IEnumerable<string> itemIds);

        // Manual pipeline control
        public RedisStorage<T> BeginPipeline()
        {
            Config.AutoExecute = false;
            GetOrCreatePipeline();
            return this;
        }

        public async Task<RedisStorage<T>> ExecutePipelineAsync()
        {
            if (_currentPipeline != null)
            {
                await _currentPipeline.ExecuteAsync();
                _currentPipeline = null;
            }
            Config.AutoExecute = true;
            return this;
        }

        public RedisStorage<T> DiscardPipeline()
        {
            _currentPipeline = null;
            Config.AutoExecute = true;
            return this;
        }

        // Common methods
        protected void ApplyTracking(T item)
        {
            if (Config.EnableTracking)
            {
                var timestampProperty = typeof(T).GetProperty(Config.TimestampProperty);
                timestampProperty?.SetValue(item, DateTime.Now);
            }
        }

        protected string Serialize(T obj) => JsonSerializer.Serialize(obj, _jsonOptions);
        protected T Deserialize(string json) => JsonSerializer.Deserialize<T>(json, _jsonOptions);
        protected string GetItemId(T item) => typeof(T).GetProperty("Id")?.GetValue(item)?.ToString();
    }

}
