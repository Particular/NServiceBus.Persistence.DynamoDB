﻿namespace NServiceBus.PersistenceTesting
{
    using System;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.Runtime;
    using NUnit.Framework;
    using Persistence.DynamoDB;
    using Persistence.DynamoDB.Tests;

    [SetUpFixture]
    public class SetupFixture
    {
        [OneTimeSetUp]
        public async Task OneTimeSetUp()
        {
            DynamoDBClient = ClientFactory.CreateDynamoDBClient();

            OutboxConfiguration = new OutboxPersistenceConfiguration()
            {
                TableName = $"{DateTime.UtcNow.Ticks}_{Path.GetFileNameWithoutExtension(Path.GetTempFileName())}_Outbox",
                TimeToLiveAttributeName = Guid.NewGuid().ToString("N") + "TTL",
                PartitionKeyName = Guid.NewGuid().ToString("N") + "PK",
                SortKeyName = Guid.NewGuid().ToString("N") + "SK",
                TimeToLive = TimeSpan.FromSeconds(100)
            };
            SagaConfiguration = new SagaPersistenceConfiguration()
            {
                TableName = $"{DateTime.UtcNow.Ticks}_{Path.GetFileNameWithoutExtension(Path.GetTempFileName())}_Saga",
                PartitionKeyName = Guid.NewGuid().ToString("N") + "PK",
                SortKeyName = Guid.NewGuid().ToString("N") + "SK"
            };

            var installer = new Installer(DynamoDBClient);

            await installer.CreateOutboxTableIfNotExists(OutboxConfiguration, CancellationToken.None).ConfigureAwait(false);
            await installer.CreateSagaTableIfNotExists(SagaConfiguration, CancellationToken.None).ConfigureAwait(false);
        }

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            await DynamoDBClient.DeleteTableAsync(SagaConfiguration.TableName).ConfigureAwait(false);
            await DynamoDBClient.DeleteTableAsync(OutboxConfiguration.TableName).ConfigureAwait(false);
            DynamoDBClient.Dispose();
        }

        public static IAmazonDynamoDB DynamoDBClient;
        public static OutboxPersistenceConfiguration OutboxConfiguration;
        public static SagaPersistenceConfiguration SagaConfiguration;
    }
}