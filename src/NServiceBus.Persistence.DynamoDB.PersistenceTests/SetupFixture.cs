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

    [SetUpFixture]
    public class SetupFixture
    {
        [OneTimeSetUp]
        public async Task OneTimeSetUp()
        {
            TableName = $"{DateTime.UtcNow.Ticks}_{Path.GetFileNameWithoutExtension(Path.GetTempFileName())}";

            var credentials = new EnvironmentVariablesAWSCredentials();
            var amazonDynamoDbConfig = new AmazonDynamoDBConfig();
            var client = new AmazonDynamoDBClient(credentials, amazonDynamoDbConfig);
            DynamoDBClient = client;

            OutboxConfiguration = new OutboxPersistenceConfiguration()
            {
                //TODO we need a dedicated outbox table while saga config doesn't support customizing PK/SK names
                TableName = $"{DateTime.UtcNow.Ticks}_{Path.GetFileNameWithoutExtension(Path.GetTempFileName())}_Outbox",
                TimeToLiveAttributeName = Guid.NewGuid().ToString("N") + "TTL",
                PartitionKeyName = Guid.NewGuid().ToString("N") + "PK",
                SortKeyName = Guid.NewGuid().ToString("N") + "SK",
                TimeToLive = TimeSpan.FromSeconds(100)
            };

            var installer = new Installer(new DynamoDBClientProvidedByConfiguration
            {
                Client = DynamoDBClient
            }, new InstallerSettings
            {
                SagaTableName = TableName,
                CreateOutboxTable = true,
                CreateSagaTable = true
            }, OutboxConfiguration);

            await installer.Install(CancellationToken.None).ConfigureAwait(false);
        }

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            await DynamoDBClient.DeleteTableAsync(TableName).ConfigureAwait(false);
            await DynamoDBClient.DeleteTableAsync(OutboxConfiguration.TableName).ConfigureAwait(false);
            DynamoDBClient.Dispose();
        }

        public static IAmazonDynamoDB DynamoDBClient;
        public static string TableName;
        public static OutboxPersistenceConfiguration OutboxConfiguration;
    }
}