using log4net.Config;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Xml;
using Topshelf;
using Topshelf.HostConfigurators;

namespace Libs.ProcessServices
{
    public sealed class ServerInit
    {
        public static void Configure(string ServiceName, string DisplayName, string Description, IEnumerable<IProcessService> processess)
        {
            XmlDocument xml = new XmlDocument();
            xml.LoadXml(System.Text.Encoding.UTF8.GetString(LibDotNet.Properties.Resources.log4net));
            XmlConfigurator.Configure(xml.DocumentElement);
            var cts = new CancellationTokenSource();
            HostFactory.Run(hostConfigurator =>
            {
                hostConfigurator.SetServiceName(ServiceName);
                hostConfigurator.SetDisplayName(string.IsNullOrEmpty(DisplayName) ? ServiceName : DisplayName);
                hostConfigurator.SetDescription(string.IsNullOrEmpty(Description) ? ServiceName : Description);
                hostConfigurator.StartAutomatically();
                hostConfigurator.RunAsLocalSystem();
                hostConfigurator.UseLog4Net();
                hostConfigurator.Service<ProcessRunner>(service =>
                {
                    service.ConstructUsing(name => new ProcessRunner(processess, cts.Token));
                    service.WhenStarted(_ => _.Start());
                    service.WhenStopped(_ => _.Stop());
                });
               //hostConfigurator.OnException(ex => { SingletonProvider<LogWriter>.Instance.WriterLogError(ex.Message, ex); });
            });
        }
    }
}
