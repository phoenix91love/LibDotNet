using System;
using System.Threading;

namespace Libs.Helpers
{
    public sealed class SingletonProvider<T> where T : new()
    {
        SingletonProvider() { }

        private static readonly Lazy<T> instance = new Lazy<T>(() => new T(), LazyThreadSafetyMode.PublicationOnly);
        public static T Instance => instance.Value;
    }
}
