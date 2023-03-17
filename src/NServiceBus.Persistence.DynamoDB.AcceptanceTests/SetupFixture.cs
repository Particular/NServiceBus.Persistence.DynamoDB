namespace NServiceBus.AcceptanceTests
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using NUnit.Framework;
    using Persistence.DynamoDB;
    using Persistence.DynamoDB.Tests;

    [SetUpFixture]
    public class SetupFixture
    {
        [OneTimeSetUp]
        public async Task OneTimeSetUp()
        {
            TableConfiguration = new TableConfiguration
            {
                TableName = $"{DateTime.UtcNow.Ticks}_{Path.GetFileNameWithoutExtension(Path.GetTempFileName())}",
            };

            DynamoDBClient = ClientFactory.CreateDynamoDBClient();

            var installer = new Installer(DynamoDBClient);

            await installer.CreateTable(TableConfiguration);
        }

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            await DynamoDBClient.DeleteTableAsync(TableConfiguration.TableName);
            DynamoDBClient.Dispose();
        }

        public static IAmazonDynamoDB DynamoDBClient;
        public static TableConfiguration TableConfiguration;
    }
}