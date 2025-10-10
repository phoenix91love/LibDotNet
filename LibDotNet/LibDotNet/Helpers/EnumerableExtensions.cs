using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Libs.Helpers
{
    public static class EnumerableExtensions
    {
        public static IEnumerable<IEnumerable<T>> Chunk<T>(this IEnumerable<T> source, int size)
        {
            if (size <= 0) throw new ArgumentException("Chunk size must be greater than 0", nameof(size));

            var list = source.ToList();
            for (int i = 0; i < list.Count; i += size)
            {
                yield return list.Skip(i).Take(size);
            }
        }

        public static List<List<T>> ChunkToList<T>(this IEnumerable<T> source, int size)
        {
            return source.Chunk(size).Select(chunk => chunk.ToList()).ToList();
        }
    }
}
