using System.Collections.Generic;
using System.Threading;
using System.Xml;
using Topshelf;
using Libs.Helpers;
namespace Libs.ProcessServices
{
    public sealed class ServerInit
    {
        public static void Configure(string ServiceName, string DisplayName, string Description, IEnumerable<IProcessService> processess)
        {
           
            var cts = new CancellationTokenSource();
            HostFactory.Run(hostConfigurator =>
            {
                hostConfigurator.SetInstanceName(ServiceName);
                hostConfigurator.SetServiceName(ServiceName);
                hostConfigurator.SetDisplayName(string.IsNullOrEmpty(DisplayName) ? ServiceName : DisplayName);
                hostConfigurator.SetDescription(string.IsNullOrEmpty(Description) ? ServiceName : Description);
                hostConfigurator.StartAutomatically();
                hostConfigurator.RunAsLocalSystem();
                hostConfigurator.Service<ProcessRunner>(service =>
                {
                    service.ConstructUsing(name => new ProcessRunner(processess, cts.Token));
                    service.WhenStarted(_ => _.Start());
                    service.WhenStopped(_ => _.Stop());
                });
                hostConfigurator.OnException(ex => { LogWriters<ServerInit>.Error(ex.Message, ex); });
                
            });
        }
    }
}
