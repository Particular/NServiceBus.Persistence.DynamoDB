namespace NServiceBus.PersistenceTesting
{
    using System;
    using System.IO;
    using System.Threading;
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
            DynamoDBClient = ClientFactory.CreateDynamoDBClient();

            var tablePrefix = $"{DateTime.UtcNow.Ticks}_{Path.GetFileNameWithoutExtension(Path.GetTempFileName())}";
            OutboxTable = new TableConfiguration
            {
                TableName = $"{tablePrefix}_Outbox",
                TimeToLiveAttributeName = Guid.NewGuid().ToString("N") + "TTL",
                PartitionKeyName = Guid.NewGuid().ToString("N") + "PK",
                SortKeyName = Guid.NewGuid().ToString("N") + "SK",
            };
            SagaTable = new TableConfiguration
            {
                TableName = $"{tablePrefix}_Saga",
                PartitionKeyName = Guid.NewGuid().ToString("N") + "PK",
                SortKeyName = Guid.NewGuid().ToString("N") + "SK",
            };

            var installer = new Installer(DynamoDBClient);

            await installer.CreateTable(OutboxTable, CancellationToken.None).ConfigureAwait(false);
            await installer.CreateTable(SagaTable, CancellationToken.None).ConfigureAwait(false);
        }

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            await DynamoDBClient.DeleteTableAsync(SagaTable.TableName).ConfigureAwait(false);
            await DynamoDBClient.DeleteTableAsync(OutboxTable.TableName).ConfigureAwait(false);
            DynamoDBClient.Dispose();
        }

        public static IAmazonDynamoDB DynamoDBClient;
        public static TableConfiguration SagaTable { get; set; }
        public static TableConfiguration OutboxTable { get; set; }
    }
}