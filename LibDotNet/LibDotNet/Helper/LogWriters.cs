using Libs.Helpers;
using NLog;
using NLog.Config;
using NLog.Targets;
using System;
using System.Collections.Concurrent;
using System.Text;
namespace LibDotNet.Helper
{
    public static class LogWriters<T> where T : class
    {
        private static Logger logger => SingletonProvider<LogConfig<T>>.Instance.GetCachedLogger();


        public static void Info(string msg) => logger.Info(msg);
        public static void Info(string msg, Exception ex) => logger.Info(msg, ex);
        public static void Info(Exception ex) => logger.Info(ex);

        public static void Warn(string msg) => logger.Warn(msg);
        public static void Warn(string msg, Exception ex) => logger.Warn(msg, ex);
        public static void Warn(Exception ex) => logger.Warn(ex);

        public static void Error(string msg) => logger.Error(msg);
        public static void Error(string msg, Exception ex) => logger.Error(msg, ex);
        public static void Error(Exception ex) => logger.Error(ex);

        public static void Fatal(string msg) => logger.Fatal(msg);
        public static void Fatal(string msg, Exception ex) => logger.Fatal(msg, ex);
        public static void Fatal(Exception ex) => logger.Fatal(ex);

    }


    internal class LogConfig<T> where T : class
    {
        private readonly object _configLock = new object();
        private volatile bool _isConfigured = false;
        private readonly ConcurrentDictionary<string, Logger> _loggerCache = new ConcurrentDictionary<string, Logger>();


        // Method để lấy logger với cache
        public Logger GetCachedLogger()
        {
            EnsureConfigured();
           
            var typeName = typeof(T).Name;
            return _loggerCache.GetOrAdd(typeName, n => LogManager.GetLogger(n));
        }

        private void EnsureConfigured()
        {
            if (!_isConfigured)
            {
                lock (_configLock)
                {
                    if (!_isConfigured)
                    {
                        ConfigureNLog();
                        _isConfigured = true;
                        var startupLogger = LogManager.GetLogger(typeof(T).Name);
                        startupLogger.Info("Logging system initialized successfully");
                    }
                }
            }
        }


        private void ConfigureNLog()
        {
            if (LogManager.Configuration != null)
                return;

            // Tạo configuration mới
            var config = new LoggingConfiguration();

            // 1. Cấu hình file chung target theo ngày với giới hạn dung lượng
            CreateGlobalFileTarget(config);

            // 1. Cấu hình file target theo ngày với giới hạn dung lượng
            CreateDailyFileTarget(config);

            // 2. Cấu hình console với màu sắc
            CreateColoredConsoleTarget(config);

            // Áp dụng configuration
            LogManager.Configuration = config;



           
        }

        private void CreateDailyFileTarget(LoggingConfiguration config)
        {
            var target = new FileTarget("dailyFile")
            {
                // File name theo ngày
                FileName = "${basedir}/logs/${logger}/${shortdate}_${logger}.log",

                // Archive configuration
                ArchiveFileName = "${basedir}/logs/${logger}/archives/${shortdate}/${shortdate}_${logger}_{##}.log",
                ArchiveAboveSize = 1024 * 1024 * 100, // 100MB
                ArchiveEvery = FileArchivePeriod.Day,
                MaxArchiveDays = 30,

                // Layout
                Layout = "${time} [${level:uppercase=true}] ${message} ${exception:format=tostring}",
                BufferSize = 512,
                CreateDirs = true,

                KeepFileOpen = false,
                Encoding = Encoding.UTF8
            };
            config.AddTarget("dailyFile", target);
            config.AddRule(LogLevel.Trace, LogLevel.Fatal, target, "*");
        }

        private void CreateGlobalFileTarget(LoggingConfiguration config)
        {
            var globalFileTarget = new FileTarget("GlobalFile")
            {
                // File log chung cho tất cả
                FileName = "${basedir}/logs/${shortdate}.log",

                ArchiveFileName = "${basedir}/logs/archives/${shortdate}/${shortdate}_{##}.log",
                ArchiveAboveSize = 1024 * 1024 * 100, // 100MB
                ArchiveEvery = FileArchivePeriod.Day,
                MaxArchiveFiles = 30,

                Layout = "[${logger}]:${time} [${level:uppercase=true}] ${message} ${exception:format=tostring}",
                BufferSize = 512,
                KeepFileOpen = false,
                Encoding = Encoding.UTF8
            };

            config.AddTarget(globalFileTarget);
            // Rule cho file chung: TẤT CẢ logger, từ Debug trở lên
            config.AddRule(LogLevel.Debug, LogLevel.Fatal, globalFileTarget, "*");
        }
        private void CreateColoredConsoleTarget(LoggingConfiguration config)
        {
            var target = new ColoredConsoleTarget("coloredConsole")
            {
                Layout = "${logger}:${time} [${level:uppercase=true}] ${message} ${exception:format=tostring}",
                UseDefaultRowHighlightingRules = false,
                Encoding = Encoding.UTF8,
            };

            // Thiết lập màu sắc theo log level
            AddConsoleHighlightingRules(target);

            config.AddTarget("coloredConsole", target);
            config.AddRule(LogLevel.Trace, LogLevel.Fatal, target, "*");
        }

        private void AddConsoleHighlightingRules(ColoredConsoleTarget target)
        {
            // Rule cho Error (đỏ trên nền trắng)
            target.RowHighlightingRules.Add(new ConsoleRowHighlightingRule
            {
                Condition = "level == LogLevel.Fatal",
                ForegroundColor = ConsoleOutputColor.Red,
                BackgroundColor = ConsoleOutputColor.Yellow,
            });

            // Rule cho Error (đỏ trên nền trắng)
            target.RowHighlightingRules.Add(new ConsoleRowHighlightingRule
            {
                Condition = "level == LogLevel.Error",
                ForegroundColor = ConsoleOutputColor.Red,
            });

            // Rule cho Warn (vàng)
            target.RowHighlightingRules.Add(new ConsoleRowHighlightingRule
            {
                Condition = "level == LogLevel.Warn",
                ForegroundColor = ConsoleOutputColor.Yellow
            });

            // Rule cho Info (trắng)
            target.RowHighlightingRules.Add(new ConsoleRowHighlightingRule
            {
                Condition = "level == LogLevel.Info",
                ForegroundColor = ConsoleOutputColor.White
            });

            // Rule cho Debug (xám)
            target.RowHighlightingRules.Add(new ConsoleRowHighlightingRule
            {
                Condition = "level == LogLevel.Debug",
                ForegroundColor = ConsoleOutputColor.Gray
            });
        }

    }
}
