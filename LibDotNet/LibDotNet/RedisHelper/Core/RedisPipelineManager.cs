using Libs.RedisHelper;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static Mysqlx.Expect.Open.Types.Condition.Types;

namespace Libs.RedisHelper
{
    public class RedisPipelineManager : IDisposable
    {
        private readonly IDatabase _db;
        private readonly List<Func<IBatch, Task>> _batchOperations;
        private readonly List<Func<ITransaction, Task>> _transactionOperations;
        private readonly RedisOperationConfig _config;
        private readonly string _key;
        private bool _executed;

        public RedisPipelineManager(IDatabase db,string key, RedisOperationConfig config)
        {
            _db = db;
            _key=key;
            _config = config;
            _batchOperations = new List<Func<IBatch, Task>>();
            _transactionOperations = new List<Func<ITransaction, Task>>();
        }
        public void AddExpiryOperation(TimeSpan expiry)
        {
            AddOperation(batch => batch.KeyExpireAsync(_key, expiry));
        }

        public void AddExpiryAtOperation(DateTime expiryAt)
        {
            AddOperation(batch => batch.KeyExpireAsync(_key, expiryAt));
        }

        public void AddPersistOperation()
        {
            AddOperation(batch => batch.KeyPersistAsync(_key));
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
}
