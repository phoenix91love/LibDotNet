using Libs.Helpers;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Libs.ProcessServices
{
    public class ProcessRunner
    {
        private readonly CancellationTokenSource _cts;
        private readonly IEnumerable<IProcessService> _processes;
        public ProcessRunner(IEnumerable<IProcessService> processes, CancellationToken ct)
        {
            _processes = processes;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        }

        public virtual void Start()
        {
            foreach (var process in _processes)
            {
                try
                {
                    LogWriters<ProcessRunner>.Info($"ProcessRunner Start {process.ProcessName}");
                    Task.Run(process.ProcessInit, _cts.Token);
                }
                catch (Exception ex)
                {
                    LogWriters<ProcessRunner>.Error($"ProcessRunner Start {process.ProcessName}", ex);
                }

            }

        }

        public virtual void Stop()
        {
            _cts.Cancel();
        }
    }

    public class ProcessServiceCore : IProcessService
    {
        // protected IDatabaseServer database = null;
        protected string ConnectionString { get; set; } = string.Empty;
        public string ProcessName => this.GetType().Name;
        public virtual void ProcessInit() { }
    }
}
