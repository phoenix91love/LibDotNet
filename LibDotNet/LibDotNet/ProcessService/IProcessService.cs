namespace Libs.ProcessServices
{
    public interface IProcessService
    {
        string ProcessName { get; }
        void ProcessInit();
    }

}
