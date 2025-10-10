using Libs.Helpers;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

public class RedisOperationConfig
{
    public bool UseTransaction { get; set; }
    public bool UseBatch { get; set; } = true;
    public int BatchSize { get; set; } = 100;
    public TimeSpan? Timeout { get; set; }
    public bool EnableTracking { get; set; }
    public string TimestampProperty { get; set; } = "UpdatedAt";
    public bool AutoExecute { get; set; } = true;
    public int MaxPipelineSize { get; set; } = 1000;
}

public static class RedisConfig
{
    public static RedisOperationConfig Optimized => new RedisOperationConfig
    {
        UseBatch = true,
        BatchSize = 100,
        AutoExecute = true
    };

    public static RedisOperationConfig Transaction => new RedisOperationConfig
    {
        UseTransaction = true,
        AutoExecute = true
    };

    public static RedisOperationConfig Batch => new RedisOperationConfig
    {
        UseBatch = true,
        BatchSize = 100,
        AutoExecute = true
    };

    public static RedisOperationConfig HighPerformance => new RedisOperationConfig
    {
        UseBatch = true,
        BatchSize = 500,
        AutoExecute = true,
        MaxPipelineSize = 5000
    };

    public static RedisOperationConfig Safe => new RedisOperationConfig
    {
        UseTransaction = true,
        Timeout = TimeSpan.FromSeconds(30),
        AutoExecute = true
    };

    public static RedisOperationConfig Custom(int batchSize = 100, bool transaction = false)
    {
        return new RedisOperationConfig
        {
            UseBatch = batchSize > 1,
            BatchSize = batchSize,
            UseTransaction = transaction
        };
    }
}

public class RedisPipelineManager : IDisposable
{
    private readonly IDatabase _db;
    private readonly List<Func<IBatch, Task>> _batchOperations;
    private readonly List<Func<ITransaction, Task>> _transactionOperations;
    private readonly RedisOperationConfig _config;
    private bool _executed;

    public RedisPipelineManager(IDatabase db, RedisOperationConfig config)
    {
        _db = db;
        _config = config;
        _batchOperations = new List<Func<IBatch, Task>>();
        _transactionOperations = new List<Func<ITransaction, Task>>();
    }

    public void AddOperation(Func<IBatch, Task> batchOperation)
    {
        if (_executed) throw new InvalidOperationException("Pipeline already executed");
        _batchOperations.Add(batchOperation);
    }

    public void AddOperation(Func<ITransaction, Task> transactionOperation)
    {
        if (_executed) throw new InvalidOperationException("Pipeline already executed");
        _transactionOperations.Add(transactionOperation);
    }

    public async Task<bool> ExecuteAsync()
    {
        if (_executed) throw new InvalidOperationException("Pipeline already executed");
        _executed = true;

        try
        {
            if (_config.UseTransaction)
            {
                return await ExecuteTransactionAsync();
            }
            else if (_config.UseBatch)
            {
                return await ExecuteBatchAsync();
            }
            else
            {
                return await ExecuteDirectAsync();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Pipeline execution failed: {ex.Message}");
            return false;
        }
    }

    private async Task<bool> ExecuteTransactionAsync()
    {
        var transaction = _db.CreateTransaction();
        var tasks = new List<Task>();

        foreach (var operation in _transactionOperations)
        {
            tasks.Add(operation(transaction));
        }

        var committed = await transaction.ExecuteAsync();
        if (committed)
        {
            await Task.WhenAll(tasks);
        }

        return committed;
    }

    private async Task<bool> ExecuteBatchAsync()
    {
        var batch = _db.CreateBatch();
        var tasks = new List<Task>();

        foreach (var operation in _batchOperations)
        {
            tasks.Add(operation(batch));
        }

        batch.Execute();
        await Task.WhenAll(tasks);
        return true;
    }

    private async Task<bool> ExecuteDirectAsync()
    {
        var batch = _db.CreateBatch();
        var tasks = new List<Task>();

        foreach (var operation in _batchOperations)
        {
            tasks.Add(operation(batch));
        }

        batch.Execute();
        await Task.WhenAll(tasks);
        return true;
    }

    public void Dispose()
    {
        if (!_executed)
        {
            _ = ExecuteAsync();
        }
    }
}
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
public static class RedisServiceExtensions
{
    // INSERT shortcuts
    public static async Task<RedisStorage<T>> I<T>(this RedisStorage<T> storage, T item, string itemId = null) where T : class, new()
    {
        return await storage.InsertAsync(item, itemId);
    }

    public static async Task<RedisStorage<T>> I<T>(this RedisStorage<T> storage, IEnumerable<T> items, Func<T, string> getId = null) where T : class, new()
    {
        return await storage.InsertAllAsync(items, getId);
    }

    // UPDATE shortcuts
    public static async Task<RedisStorage<T>> U<T>(this RedisStorage<T> storage, T item, string itemId = null) where T : class, new()
    {
        return await storage.UpdateAsync(item, itemId);
    }

    public static async Task<RedisStorage<T>> U<T>(this RedisStorage<T> storage, string itemId, Action<T> updateAction) where T : class, new()
    {
        return await storage.UpdateAsync(itemId, updateAction);
    }

    public static async Task<RedisStorage<T>> U<T>(this RedisStorage<T> storage, IEnumerable<T> items, Func<T, string> getId = null) where T : class, new()
    {
        return await storage.UpdateAllAsync(items, getId);
    }

    public static async Task<RedisStorage<T>> P<T>(this RedisStorage<T> storage, string itemId, string property, object value) where T : class, new()
    {
        return await storage.UpdatePropertyAsync(itemId, property, value);
    }

    // DELETE shortcuts
    public static async Task<RedisStorage<T>> D<T>(this RedisStorage<T> storage, string itemId) where T : class, new()
    {
        return await storage.DeleteAsync(itemId);
    }

    public static async Task<RedisStorage<T>> D<T>(this RedisStorage<T> storage, IEnumerable<string> itemIds) where T : class, new()
    {
        return await storage.DeleteAllAsync(itemIds);
    }

    public static async Task<RedisStorage<T>> Clear<T>(this RedisStorage<T> storage) where T : class, new()
    {
        return await storage.ClearAsync();
    }

    // GET shortcuts
    public static async Task<T> G<T>(this RedisStorage<T> storage, string itemId) where T : class, new()
    {
        return await storage.GetAsync(itemId);
    }

    public static async Task<List<T>> G<T>(this RedisStorage<T> storage) where T : class, new()
    {
        return await storage.GetAllAsync();
    }

    public static async Task<bool> E<T>(this RedisStorage<T> storage, string itemId) where T : class, new()
    {
        return await storage.ExistsAsync(itemId);
    }

    public static async Task<long> C<T>(this RedisStorage<T> storage) where T : class, new()
    {
        return await storage.CountAsync();
    }

    // QUERY shortcuts
    public static async Task<List<T>> F<T>(this RedisStorage<T> storage, Func<T, bool> filter) where T : class, new()
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
    public static RedisStorage<T> Txn<T>(this RedisStorage<T> storage) where T : class, new()
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
    public static async Task<RedisStorage<T>> US<T>(this RedisStorage<T> storage, T item, string itemId = null) where T : class, new()
    {
        if (storage is RedisHashStorage<T> hashStorage)
        {
            return await hashStorage.UpsertAsync(item, itemId);
        }
        throw new NotSupportedException("Upsert only supported for Hash storage");
    }

    public static async Task<RedisStorage<T>> Inc<T>(this RedisStorage<T> storage, string itemId, string property, long value = 1) where T : class, new()
    {
        if (storage is RedisHashStorage<T> hashStorage)
        {
            return await hashStorage.IncrementAsync(itemId, property, value);
        }
        throw new NotSupportedException("Increment only supported for Hash storage");
    }

    public static async Task<RedisStorage<T>> UIf<T>(this RedisStorage<T> storage, string itemId, Func<T, bool> condition, Action<T> updateAction) where T : class, new()
    {
        if (storage is RedisHashStorage<T> hashStorage)
        {
            return await hashStorage.UpdateIfAsync(itemId, condition, updateAction);
        }
        throw new NotSupportedException("Conditional update only supported for Hash storage");
    }
}
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
public class RedisStringStorage<T> : RedisStorage<T> where T : class, new()
{
    public RedisStringStorage(IDatabase db, string key) : base(db, key) { }

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
        AddToPipeline(batch => InsertAllInPipeline(batch, itemList, getId));
        await ExecutePipelineIfNeeded();
        return this;
    }

    protected override Task InsertInPipeline(IBatch batch, T item, string itemId)
    {
        ApplyTracking(item);
        return batch.StringSetAsync(_key, Serialize(item)); // ✅ Đúng: Serialize single item
    }

    protected override Task InsertAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId)
    {
        var itemList = items.ToList();
        ApplyTrackingToAll(itemList);
        // ✅ Sửa: Serialize entire list as JSON array
        var json = JsonSerializer.Serialize(itemList, _jsonOptions);
        return batch.StringSetAsync(_key, json);
    }
    #endregion

    #region UPDATE Operations
    public override async Task<RedisStorage<T>> UpdateAsync(T item, string itemId = null)
    {
        AddToPipeline(batch => UpdateInPipeline(batch, item, itemId));
        await ExecutePipelineIfNeeded();
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
        return this;
    }

    public override async Task<RedisStorage<T>> UpdatePropertyAsync(string itemId, string propertyName, object value)
    {
        var currentItem = await GetAsync();
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
        return batch.StringSetAsync(_key, Serialize(item)); // ✅ Đúng: Serialize single item
    }

    protected override Task UpdateAllInPipeline(IBatch batch, IEnumerable<T> items, Func<T, string> getId)
    {
        var itemList = items.ToList();
        ApplyTrackingToAll(itemList);
        // ✅ Sửa: Serialize entire list as JSON array
        var json = JsonSerializer.Serialize(itemList, _jsonOptions);
        return batch.StringSetAsync(_key, json);
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

            // Update the entire collection
            var json = JsonSerializer.Serialize(allItems, _jsonOptions);
            await _db.StringSetAsync(_key, json);
        }
        return this;
    }
    #endregion

    #region DELETE Operations
    public override async Task<RedisStorage<T>> DeleteAsync(string itemId)
    {
        // For string storage storing single object, delete the entire key
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
        }
        return this;
    }

    protected override Task DeleteInPipeline(IBatch batch, string itemId)
    {
        return batch.KeyDeleteAsync(_key);
    }

    protected override Task DeleteAllInPipeline(IBatch batch, IEnumerable<string> itemIds)
    {
        // This is handled in DeleteAllAsync
        return Task.CompletedTask;
    }

    // Delete item from collection
    public async Task<RedisStorage<T>> DeleteFromCollectionAsync(string itemId)
    {
        var allItems = await GetAllAsync();
        var updatedItems = allItems.Where(item => GetItemId(item) != itemId).ToList();

        var json = JsonSerializer.Serialize(updatedItems, _jsonOptions);
        await _db.StringSetAsync(_key, json);

        return this;
    }

    public async Task<RedisStorage<T>> ClearAsync()
    {
        await ExecutePipelineIfNeeded();
        await _db.KeyDeleteAsync(_key);
        return this;
    }
    #endregion

    #region GET Operations
    public override async Task<T> GetAsync(string itemId = null)
    {
        await ExecutePipelineIfNeeded();
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

    public override async Task<bool> ExistsAsync(string itemId = null)
    {
        await ExecutePipelineIfNeeded();
        return await _db.KeyExistsAsync(_key);
    }

    public override async Task<long> CountAsync()
    {
        var allItems = await GetAllAsync();
        return allItems.Count;
    }

    // Get specific item from collection
    public async Task<T> GetFromCollectionAsync(string itemId)
    {
        var allItems = await GetAllAsync();
        return allItems.FirstOrDefault(item => GetItemId(item) == itemId);
    }
    #endregion

    #region Utility Methods
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
        return this;
    }
    #endregion
}
public static class RedisStorageTypeExtensions
{
    #region String Storage Extensions
    public static async Task<RedisStorage<T>> Append<T>(this RedisStorage<T> storage, T item) where T : class, new()
    {
        if (storage is RedisStringStorage<T> stringStorage)
        {
            return await stringStorage.AppendAsync(item);
        }
        throw new NotSupportedException("Append only supported for String storage");
    }

    public static async Task<T> GetSingle<T>(this RedisStorage<T> storage) where T : class, new()
    {
        if (storage is RedisStringStorage<T> stringStorage)
        {
            return await stringStorage.GetAsync();
        }
        throw new NotSupportedException("GetSingle only supported for String storage");
    }

    public static async Task<RedisStorage<T>> UpdateInCollection<T>(this RedisStorage<T> storage, string itemId, Action<T> updateAction) where T : class, new()
    {
        if (storage is RedisStringStorage<T> stringStorage)
        {
            return await stringStorage.UpdateInCollectionAsync(itemId, updateAction);
        }
        throw new NotSupportedException("UpdateInCollection only supported for String storage");
    }

    public static async Task<RedisStorage<T>> DeleteFromCollection<T>(this RedisStorage<T> storage, string itemId) where T : class, new()
    {
        if (storage is RedisStringStorage<T> stringStorage)
        {
            return await stringStorage.DeleteFromCollectionAsync(itemId);
        }
        throw new NotSupportedException("DeleteFromCollection only supported for String storage");
    }

    public static async Task<T> GetFromCollection<T>(this RedisStorage<T> storage, string itemId) where T : class, new()
    {
        if (storage is RedisStringStorage<T> stringStorage)
        {
            return await stringStorage.GetFromCollectionAsync(itemId);
        }
        throw new NotSupportedException("GetFromCollection only supported for String storage");
    }
    #endregion

    #region Individual Storage Extensions
    public static async Task<List<T>> GetMultiple<T>(this RedisStorage<T> storage, IEnumerable<string> itemIds) where T : class, new()
    {
        if (storage is RedisIndividualStorage<T> individualStorage)
        {
            return await individualStorage.GetMultipleAsync(itemIds);
        }
        throw new NotSupportedException("GetMultiple only supported for Individual storage");
    }

    public static async Task<List<string>> GetKeys<T>(this RedisStorage<T> storage) where T : class, new()
    {
        if (storage is RedisIndividualStorage<T> individualStorage)
        {
            return await individualStorage.GetAllKeysAsync();
        }
        throw new NotSupportedException("GetKeys only supported for Individual storage");
    }

    public static async Task<RedisStorage<T>> ClearAll<T>(this RedisStorage<T> storage) where T : class, new()
    {
        if (storage is RedisIndividualStorage<T> individualStorage)
        {
            return await individualStorage.ClearAllAsync();
        }
        throw new NotSupportedException("ClearAll only supported for Individual storage");
    }

    public static async Task<List<T>> Search<T>(this RedisStorage<T> storage, string propertyName, object value) where T : class, new()
    {
        if (storage is RedisIndividualStorage<T> individualStorage)
        {
            return await individualStorage.SearchByPropertyAsync(propertyName, value);
        }
        throw new NotSupportedException("Search only supported for Individual storage");
    }

    public static async Task<RedisStorage<T>> BulkUpdateProps<T>(this RedisStorage<T> storage, Dictionary<string, Dictionary<string, object>> updates) where T : class, new()
    {
        if (storage is RedisIndividualStorage<T> individualStorage)
        {
            return await individualStorage.BulkUpdatePropertiesAsync(updates);
        }
        throw new NotSupportedException("BulkUpdateProps only supported for Individual storage");
    }
    #endregion
}

public static class CompleteStorageExtensions
{
    #region String Storage Extensions
    public static async Task<RedisStorage<T>> Append<T>(this RedisStorage<T> storage, T item) where T : class, new()
    {
        if (storage is RedisStringStorage<T> stringStorage)
        {
            return await stringStorage.AppendAsync(item);
        }
        throw new NotSupportedException("Append only supported for String storage");
    }

    public static async Task<T> GetSingle<T>(this RedisStorage<T> storage) where T : class, new()
    {
        if (storage is RedisStringStorage<T> stringStorage)
        {
            return await stringStorage.GetAsync();
        }
        throw new NotSupportedException("GetSingle only supported for String storage");
    }

    public static async Task<RedisStorage<T>> UpdateInCollection<T>(this RedisStorage<T> storage, string itemId, Action<T> updateAction) where T : class, new()
    {
        if (storage is RedisStringStorage<T> stringStorage)
        {
            return await stringStorage.UpdateInCollectionAsync(itemId, updateAction);
        }
        throw new NotSupportedException("UpdateInCollection only supported for String storage");
    }

    public static async Task<RedisStorage<T>> DeleteFromCollection<T>(this RedisStorage<T> storage, string itemId) where T : class, new()
    {
        if (storage is RedisStringStorage<T> stringStorage)
        {
            return await stringStorage.DeleteFromCollectionAsync(itemId);
        }
        throw new NotSupportedException("DeleteFromCollection only supported for String storage");
    }

    public static async Task<T> GetFromCollection<T>(this RedisStorage<T> storage, string itemId) where T : class, new()
    {
        if (storage is RedisStringStorage<T> stringStorage)
        {
            return await stringStorage.GetFromCollectionAsync(itemId);
        }
        throw new NotSupportedException("GetFromCollection only supported for String storage");
    }
    #endregion

    #region Individual Storage Extensions
    public static async Task<List<T>> GetMultiple<T>(this RedisStorage<T> storage, IEnumerable<string> itemIds) where T : class, new()
    {
        if (storage is RedisIndividualStorage<T> individualStorage)
        {
            return await individualStorage.GetMultipleAsync(itemIds);
        }
        throw new NotSupportedException("GetMultiple only supported for Individual storage");
    }

    public static async Task<List<string>> GetKeys<T>(this RedisStorage<T> storage) where T : class, new()
    {
        if (storage is RedisIndividualStorage<T> individualStorage)
        {
            return await individualStorage.GetAllKeysAsync();
        }
        throw new NotSupportedException("GetKeys only supported for Individual storage");
    }

    public static async Task<RedisStorage<T>> ClearAll<T>(this RedisStorage<T> storage) where T : class, new()
    {
        if (storage is RedisIndividualStorage<T> individualStorage)
        {
            return await individualStorage.ClearAllAsync();
        }
        throw new NotSupportedException("ClearAll only supported for Individual storage");
    }

    public static async Task<List<T>> Search<T>(this RedisStorage<T> storage, string propertyName, object value) where T : class, new()
    {
        if (storage is RedisIndividualStorage<T> individualStorage)
        {
            return await individualStorage.SearchByPropertyAsync(propertyName, value);
        }
        throw new NotSupportedException("Search only supported for Individual storage");
    }

    public static async Task<RedisStorage<T>> BulkUpdateProps<T>(this RedisStorage<T> storage, Dictionary<string, Dictionary<string, object>> updates) where T : class, new()
    {
        if (storage is RedisIndividualStorage<T> individualStorage)
        {
            return await individualStorage.BulkUpdatePropertiesAsync(updates);
        }
        throw new NotSupportedException("BulkUpdateProps only supported for Individual storage");
    }

    public static async Task<RedisStorage<T>> US<T>(this RedisStorage<T> storage, T item, string itemId = null) where T : class, new()
    {
        if (storage is RedisIndividualStorage<T> individualStorage)
        {
            return await individualStorage.UpsertAsync(item, itemId);
        }
        else if (storage is RedisHashStorage<T> hashStorage)
        {
            return await hashStorage.UpsertAsync(item, itemId);
        }
        throw new NotSupportedException("Upsert only supported for Hash and Individual storage");
    }
    #endregion

    #region List Storage Extensions
    public static async Task<RedisStorage<T>> Left<T>(this RedisStorage<T> storage, T item) where T : class, new()
    {
        if (storage is RedisListStorage<T> listStorage)
        {
            return await listStorage.InsertLeftAsync(item);
        }
        throw new NotSupportedException("Left insert only supported for List storage");
    }

    public static async Task<RedisStorage<T>> At<T>(this RedisStorage<T> storage, int index, T item) where T : class, new()
    {
        if (storage is RedisListStorage<T> listStorage)
        {
            return await listStorage.InsertAtAsync(index, item);
        }
        throw new NotSupportedException("Insert at index only supported for List storage");
    }

    public static async Task<T> At<T>(this RedisStorage<T> storage, int index) where T : class, new()
    {
        if (storage is RedisListStorage<T> listStorage)
        {
            return await listStorage.GetAtAsync(index);
        }
        throw new NotSupportedException("Get at index only supported for List storage");
    }

    public static async Task<List<T>> Range<T>(this RedisStorage<T> storage, int start, int stop) where T : class, new()
    {
        if (storage is RedisListStorage<T> listStorage)
        {
            return await listStorage.GetRangeAsync(start, stop);
        }
        throw new NotSupportedException("Get range only supported for List storage");
    }

    public static async Task<RedisStorage<T>> RemoveAt<T>(this RedisStorage<T> storage, int index) where T : class, new()
    {
        if (storage is RedisListStorage<T> listStorage)
        {
            return await listStorage.DeleteAtAsync(index);
        }
        throw new NotSupportedException("Delete at index only supported for List storage");
    }

    public static async Task<T> PopLeft<T>(this RedisStorage<T> storage) where T : class, new()
    {
        if (storage is RedisListStorage<T> listStorage)
        {
            return await listStorage.PopLeftAsync();
        }
        throw new NotSupportedException("Pop left only supported for List storage");
    }

    public static async Task<T> PopRight<T>(this RedisStorage<T> storage) where T : class, new()
    {
        if (storage is RedisListStorage<T> listStorage)
        {
            return await listStorage.PopRightAsync();
        }
        throw new NotSupportedException("Pop right only supported for List storage");
    }
    #endregion

    #region Hash Storage Extensions
    public static async Task<RedisStorage<T>> Inc<T>(this RedisStorage<T> storage, string itemId, string property, long value = 1) where T : class, new()
    {
        if (storage is RedisHashStorage<T> hashStorage)
        {
            return await hashStorage.IncrementAsync(itemId, property, value);
        }
        throw new NotSupportedException("Increment only supported for Hash storage");
    }

    public static async Task<RedisStorage<T>> UIf<T>(this RedisStorage<T> storage, string itemId, Func<T, bool> condition, Action<T> updateAction) where T : class, new()
    {
        if (storage is RedisHashStorage<T> hashStorage)
        {
            return await hashStorage.UpdateIfAsync(itemId, condition, updateAction);
        }
        throw new NotSupportedException("Conditional update only supported for Hash storage");
    }
    #endregion
}