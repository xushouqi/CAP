using System.Threading.Tasks;
using DotNetCore.CAP.Models;

namespace DotNetCore.CAP
{
    public interface IQueueExecutor
    {
        Task<OperateResult> ExecuteAsync(IStorageConnection connection, IFetchedMessage message);

        //xu
        Task<OperateResult> PublishAsync(string keyName, string content, bool saveToDb);
        Task<OperateResult> ExecuteSubscribeAsync(CapReceivedMessage receivedMessage);
    }
}