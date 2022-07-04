namespace NServiceBus.PersistenceTesting
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

            var installer = new Installer(new DynamoDBClientProvidedByConfiguration
            {
                Client = DynamoDBClient
            }, new InstallerSettings
            {
                OutboxTableName = TableName,
            });

            await installer.Install("", CancellationToken.None).ConfigureAwait(false);
        }

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            await DynamoDBClient.DeleteTableAsync(TableName).ConfigureAwait(false);
            DynamoDBClient.Dispose();
        }

        public static AmazonDynamoDBClient DynamoDBClient;
        public static string TableName;
    }
}