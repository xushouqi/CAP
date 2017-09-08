using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using DotNetCore.CAP.Models;

namespace DotNetCore.CAP.RabbitMQ
{
    internal sealed class RabbitMQConsumerClient : IConsumerClient
    {
        private readonly string _exchageName;
        private readonly string _queueName;
        private readonly RabbitMQOptions _rabbitMQOptions;

        private ConnectionPool _connectionPool;
        private IModel _channel;
        private ulong _deliveryTag;

        public event EventHandler<MessageContext> OnMessageReceieved;

        public event EventHandler<string> OnError;

        private readonly IQueueExecutorFactory _queueExecutorFactory;

        public RabbitMQConsumerClient(string queueName,
             ConnectionPool connectionPool,
               IQueueExecutorFactory queueExecutorFactory,
             RabbitMQOptions options)
        {
            _queueName = queueName;
            _connectionPool = connectionPool;
            _rabbitMQOptions = options;
            _exchageName = options.TopicExchangeName;
            _queueExecutorFactory = queueExecutorFactory;

            InitClient();
        }

        private void InitClient()
        {
            var connection = _connectionPool.Rent();

            _channel = connection.CreateModel();

            _channel.ExchangeDeclare(
                exchange: _exchageName,
                type: RabbitMQOptions.ExchangeType,
                durable: true);

            var arguments = new Dictionary<string, object> { { "x-message-ttl", (int)_rabbitMQOptions.QueueMessageExpires } };
            _channel.QueueDeclare(_queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: arguments);

            _connectionPool.Return(connection);
        }

        public void Subscribe(IEnumerable<string> topics)
        {
            if (topics == null) throw new ArgumentNullException(nameof(topics));

            foreach (var topic in topics)
            {
                _channel.QueueBind(_queueName, _exchageName, topic);
            }
        }

        public void Listening(TimeSpan timeout, CancellationToken cancellationToken)
        {
            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += OnConsumerReceived;
            consumer.Shutdown += OnConsumerShutdown;
            _channel.BasicConsume(_queueName, false, consumer);
            while (true)
            {
                Task.Delay(timeout, cancellationToken).GetAwaiter().GetResult();
            }
        }

        public void Commit()
        {
            _channel.BasicAck(_deliveryTag, false);
        }

        public void Dispose()
        {
            _channel.Dispose();
        }

        private void OnConsumerReceived(object sender, BasicDeliverEventArgs e)
        {
            _deliveryTag = e.DeliveryTag;
            var content = Encoding.UTF8.GetString(e.Body);
            var saveToDb = "1";
            int idx = content.LastIndexOf("#DB#");
            if (idx >= 0)
            {
                content = content.Substring(0, idx);
                saveToDb = content.Substring(idx + "#DB#".Length);
            }
            var message = new MessageContext
            {
                Group = _queueName,
                Name = e.RoutingKey,
                Content = content,
            };
            if (saveToDb.Equals("1"))
                OnMessageReceieved?.Invoke(sender, message);
            else
            {
                var receivedMessage = new CapReceivedMessage(message);
                //xu: 直接接受派送订阅者
                var queueExecutor = _queueExecutorFactory.GetInstance(MessageType.Subscribe);
                queueExecutor.ExecuteSubscribeAsync(receivedMessage).Wait();
            }
        }

        private void OnConsumerShutdown(object sender, ShutdownEventArgs e)
        {
            OnError?.Invoke(sender, e.Cause?.ToString());
        }
    }
}