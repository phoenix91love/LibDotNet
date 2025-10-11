using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Libs.RedisHelper
{
 
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
}
