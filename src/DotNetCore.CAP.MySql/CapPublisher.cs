﻿using System;
using System.Data;
using System.Threading.Tasks;
using Dapper;
using DotNetCore.CAP.Abstractions;
using DotNetCore.CAP.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;

namespace DotNetCore.CAP.MySql
{
    public class CapPublisher : CapPublisherBase, ICallbackPublisher
    {
        private readonly ILogger _logger;
        private readonly MySqlOptions _options;
        private readonly DbContext _dbContext;
        private readonly IQueueExecutorFactory _queueExecutorFactory;

        public CapPublisher(IServiceProvider provider,
            ILogger<CapPublisher> logger,
               IQueueExecutorFactory queueExecutorFactory,
            MySqlOptions options)
        {
            ServiceProvider = provider;
            _options = options;
            _logger = logger;
            _queueExecutorFactory = queueExecutorFactory;

            if (_options.DbContextType != null)
            {
                IsUsingEF = true;
                _dbContext = (DbContext)ServiceProvider.GetService(_options.DbContextType);
            }
        }

        protected override void PrepareConnectionForEF()
        {
            DbConnection = _dbContext.Database.GetDbConnection();
            var dbContextTransaction = _dbContext.Database.CurrentTransaction;
            var dbTrans = dbContextTransaction?.GetDbTransaction();
            //DbTransaction is dispose in original
            if (dbTrans?.Connection == null)
            {
                IsCapOpenedTrans = true;
                dbContextTransaction?.Dispose();
                dbContextTransaction = _dbContext.Database.BeginTransaction(IsolationLevel.ReadCommitted);
                dbTrans = dbContextTransaction.GetDbTransaction();
            }
            DbTranasaction = dbTrans;
        }

        protected override void Execute(IDbConnection dbConnection, IDbTransaction dbTransaction, CapPublishedMessage message)
        {
            dbConnection.Execute(PrepareSql(), message, dbTransaction);

            _logger.LogInformation("Published Message has been persisted in the database. name:" + message.ToString());
        }

        protected override async Task ExecuteAsync(IDbConnection dbConnection, IDbTransaction dbTransaction, CapPublishedMessage message)
        {
            await dbConnection.ExecuteAsync(PrepareSql(), message, dbTransaction);

            _logger.LogInformation("Published Message has been persisted in the database. name:" + message.ToString());
        }

        public async Task PublishAsync(CapPublishedMessage message)
        {
            if (message.SaveToDb)
            {
                using (var conn = new MySqlConnection(_options.ConnectionString))
                {
                    await conn.ExecuteAsync(PrepareSql(), message);
                }
            }
            else
            {
                //xu: 直接发送
                var queueExecutor = _queueExecutorFactory.GetInstance(MessageType.Publish);
                await queueExecutor.PublishAsync(message.Name, message.Content, false);
            }
        }

        #region private methods

        private string PrepareSql()
        {
            return $"INSERT INTO `{_options.TableNamePrefix}.published` (`Name`,`Content`,`Retries`,`Added`,`ExpiresAt`,`StatusName`)VALUES(@Name,@Content,@Retries,@Added,@ExpiresAt,@StatusName)";
        }

        #endregion private methods
    }
}