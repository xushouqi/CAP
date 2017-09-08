﻿using System;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using DotNetCore.CAP.Processor.States;
using Microsoft.Extensions.Logging;

namespace DotNetCore.CAP.Kafka
{
    internal class PublishQueueExecutor : BasePublishQueueExecutor
    {
        private readonly ILogger _logger;
        private readonly KafkaOptions _kafkaOptions;

        public PublishQueueExecutor(
            CapOptions options,
            IStateChanger stateChanger,
            KafkaOptions kafkaOptions,
            ILogger<PublishQueueExecutor> logger)
            : base(options, stateChanger, logger)
        {
            _logger = logger;
            _kafkaOptions = kafkaOptions;
        }

        public override Task<OperateResult> PublishAsync(string keyName, string content, bool saveToDb)
        {
            try
            {
                var config = _kafkaOptions.AskafkaConfig();
                var contentBytes = Encoding.UTF8.GetBytes(content);
                using (var producer = new Producer(config))
                {
                    var message = producer.ProduceAsync(keyName, null, contentBytes).Result;

                    if (!message.Error.HasError)
                    {
                        _logger.LogDebug($"kafka topic message [{keyName}] has been published.");

                        return Task.FromResult(OperateResult.Success);
                    }
                    else
                    {
                        return Task.FromResult(OperateResult.Failed(new OperateError
                        {
                            Code = message.Error.Code.ToString(),
                            Description = message.Error.Reason
                        }));
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"kafka topic message [{keyName}] has benn raised an exception of sending. the exception is: {ex.Message}");

                return Task.FromResult(OperateResult.Failed(ex));
            }
        }
    }
}