﻿namespace NServiceBus.AcceptanceTests
{
    using System;
    using System.IO;
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

            var installer = new Installer(DynamoDBClient);

            await installer.CreateOutboxTableIfNotExists(new OutboxPersistenceConfiguration { TableName = TableName });
            await installer.CreateSagaTableIfNotExists(new SagaPersistenceConfiguration { TableName = TableName });
        }

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            await DynamoDBClient.DeleteTableAsync(TableName);
            DynamoDBClient.Dispose();
        }

        public static string TableName;
        public static AmazonDynamoDBClient DynamoDBClient;
    }
}