using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Libs.RedisHelper
{

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
}
