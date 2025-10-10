using System;
using System.Collections.Generic;
using System.Text;

namespace Libs.RedisHelper
{
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
}
