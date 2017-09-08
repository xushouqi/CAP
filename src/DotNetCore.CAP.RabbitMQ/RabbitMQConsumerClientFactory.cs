using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace DotNetCore.CAP.RabbitMQ
{
    internal sealed class RabbitMQConsumerClientFactory : IConsumerClientFactory
    {
        private readonly RabbitMQOptions _rabbitMQOptions;
        private readonly ConnectionPool _connectionPool;
        private readonly IQueueExecutorFactory _queueExecutorFactory;


        public RabbitMQConsumerClientFactory(RabbitMQOptions rabbitMQOptions,
               IQueueExecutorFactory queueExecutorFactory,
                ConnectionPool pool)
        {
            _rabbitMQOptions = rabbitMQOptions;
            _connectionPool = pool;
            _queueExecutorFactory = queueExecutorFactory;
        }

        public IConsumerClient Create(string groupId)
        {
            return new RabbitMQConsumerClient(groupId, _connectionPool, _queueExecutorFactory, _rabbitMQOptions);
        }
    }
}