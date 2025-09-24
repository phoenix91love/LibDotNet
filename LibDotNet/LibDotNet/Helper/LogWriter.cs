using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Util;
using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml;

namespace Libs.Helpers
{
    public sealed class LogWriter
    {
        private ILog Logger = null;
        public LogWriter()
        {
           // XmlConfigurator.Configure(GetElement(LibDotNet.Properties.Resources.log4net));
            this.Logger = LogManager.GetLogger(typeof(LogWriter));
        }

        public void WriterLogInfo(string msg) => this.Logger.Info($"{GetMethodCaller}===> {msg}");
        public void WriterLogInfo(string msg, Exception ex) => this.Logger.Info($"{GetMethodCaller} ===> {msg}", ex);
        public void WriterLogInfo(Exception ex) => WriterLogWarning($"{GetMethodCaller} ===>", ex);

        public void WriterLogWarning(string msg) => this.Logger.Warn($"{GetMethodCaller} ===> {msg}");
        public void WriterLogWarning(string msg, Exception ex) => this.Logger.Warn($"{GetMethodCaller} ===> {msg}", ex);
        public void WriterLogWarning(Exception ex) => WriterLogWarning($"{GetMethodCaller} ===>", ex);

        public void WriterLogError(string msg) => this.Logger.Error($"{GetMethodCaller} ===> {msg}");
        public void WriterLogError(string msg, Exception ex) => Logger.Error($"{GetMethodCaller} ===> {msg}", ex);
        public void WriterLogError(Exception ex) => WriterLogWarning($"{GetMethodCaller} ===>", ex);

        public void WriterLogFatal(string msg) => this.Logger.Fatal($"{GetMethodCaller} ===> {msg}");
        public void WriterLogFatal(string msg, Exception ex) => Logger.Fatal($"{GetMethodCaller} ===> {msg}", ex);
        public void WriterLogFatal(Exception ex) => WriterLogFatal($"{GetMethodCaller} ===>", ex);


        private string GetMethodCaller => $"Class:{new StackTrace().GetFrame(2).GetMethod().DeclaringType.Name} Method:{new StackTrace().GetFrame(2).GetMethod().Name}";

        private XmlElement GetElement(string xml)
        {
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(xml);
            return doc.DocumentElement;
        }
    }

    public class RollingFileAppenderExtent : RollingFileAppender
    {

        protected override void OpenFile(string fileName, bool append)
        {
            string baseDirectory = System.AppDomain.CurrentDomain.BaseDirectory;
            string fileNameOnly = Path.GetFileName(fileName);
            string newDirectory = Path.Combine(baseDirectory, "Log");
            if (!Directory.Exists(newDirectory))
                Directory.CreateDirectory(newDirectory);
            string newFileName = Path.Combine(newDirectory, fileNameOnly);

            base.OpenFile(newFileName, append);
        }
    }
}
