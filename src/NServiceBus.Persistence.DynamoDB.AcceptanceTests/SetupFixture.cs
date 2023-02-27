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

            var installer = new Installer(new DynamoDBClientProvidedByConfiguration
            {
                Client = DynamoDBClient
            }, new InstallerSettings
            {
                Disabled = false,
                CreateOutboxTable = true,
                CreateSagaTable = true
            }, new OutboxPersistenceConfiguration
            {
                TableName = TableName
            }, new SagaPersistenceConfiguration
            {
                TableName = TableName
            });

            await installer.Install();
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