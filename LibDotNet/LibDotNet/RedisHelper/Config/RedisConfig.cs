using System;
using System.Collections.Generic;
using System.Text;

namespace Libs.RedisHelper
{

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
        public static RedisOperationConfig WithExpiry(TimeSpan expiry, bool sliding = false)
        {
            return new RedisOperationConfig
            {
                UseBatch = true,
                BatchSize = 100,
                AutoExecute = true,
                DefaultExpiry = expiry,
                SlidingExpiration = sliding
            };
        }

        public static RedisOperationConfig ShortLived => new RedisOperationConfig
        {
            UseBatch = true,
            DefaultExpiry = TimeSpan.FromMinutes(30),
            SlidingExpiration = true
        };

        public static RedisOperationConfig Session => new RedisOperationConfig
        {
            UseBatch = true,
            DefaultExpiry = TimeSpan.FromHours(2),
            SlidingExpiration = true
        };

        public static RedisOperationConfig Cache => new RedisOperationConfig
        {
            UseBatch = true,
            DefaultExpiry = TimeSpan.FromMinutes(10),
            SlidingExpiration = false
        };

        public static RedisOperationConfig Permanent => new RedisOperationConfig
        {
            UseBatch = true,
            DefaultExpiry = null, // No expiry
            SlidingExpiration = false
        };
    }

}
