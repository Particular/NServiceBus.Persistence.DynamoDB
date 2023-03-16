namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System;
    using System.Linq;
    using System.Threading;
    using Amazon.DynamoDBv2.Model;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using NUnit.Framework;

    //TODO would be nice to be able to tag the tables as tests for easier identification in AWS
    [TestFixture]
    public class InstallerTests
    {
        IAmazonDynamoDB dynamoClient;
        Installer installer;
        TableConfiguration tableConfiguration;

        [SetUp]
        public void Setup()
        {
            dynamoClient = ClientFactory.CreateDynamoDBClient();
            tableConfiguration = new TableConfiguration
            {
                TableName = Guid.NewGuid().ToString("N") + "_installer_tests",
                PartitionKeyName = Guid.NewGuid().ToString("N"),
                SortKeyName = Guid.NewGuid().ToString("N")
            };
            installer = new Installer(dynamoClient);
        }

        [TearDown]
        public async Task Teardown()
        {
            try
            {
                await dynamoClient.DeleteTableAsync(tableConfiguration.TableName);
            }
            catch (Exception e)
            {
                TestContext.WriteLine($"Error deleting queue: {e}");
            }
        }

        [Test]
        public async Task Should_create_table_with_configured_attribute_names()
        {
            tableConfiguration.PartitionKeyName = Guid.NewGuid().ToString("N");
            tableConfiguration.SortKeyName = Guid.NewGuid().ToString("N");

            await installer.CreateTable(tableConfiguration, CancellationToken.None);

            var table = await dynamoClient.DescribeTableAsync(tableConfiguration.TableName);

            Assert.AreEqual(tableConfiguration.TableName, table.Table.TableName);
            Assert.AreEqual(tableConfiguration.PartitionKeyName, table.Table.KeySchema[0].AttributeName);
            Assert.AreEqual(KeyType.HASH, table.Table.KeySchema[0].KeyType);
            Assert.AreEqual(ScalarAttributeType.S, table.Table.AttributeDefinitions.Single(a => a.AttributeName == tableConfiguration.PartitionKeyName).AttributeType);
            Assert.AreEqual(tableConfiguration.SortKeyName, table.Table.KeySchema[1].AttributeName);
            Assert.AreEqual(KeyType.RANGE, table.Table.KeySchema[1].KeyType);
            Assert.AreEqual(ScalarAttributeType.S, table.Table.AttributeDefinitions.Single(a => a.AttributeName == tableConfiguration.SortKeyName).AttributeType);
            Assert.AreEqual(TableStatus.ACTIVE, table.Table.TableStatus);
        }

        [Test]
        public async Task Should_create_table_with_pay_as_you_go_billing_mode()
        {
            tableConfiguration.BillingMode = BillingMode.PAY_PER_REQUEST;

            await installer.CreateTable(tableConfiguration, CancellationToken.None);

            var table = await dynamoClient.DescribeTableAsync(tableConfiguration.TableName);

            Assert.AreEqual(BillingMode.PAY_PER_REQUEST, table.Table.BillingModeSummary.BillingMode);
            Assert.AreEqual(0, table.Table.ProvisionedThroughput.ReadCapacityUnits);
            Assert.AreEqual(0, table.Table.ProvisionedThroughput.WriteCapacityUnits);
        }

        [Test]
        public async Task Should_create_table_with_provisioned_billing_mode()
        {
            tableConfiguration.ProvisionedThroughput = new ProvisionedThroughput(1, 1);
            tableConfiguration.BillingMode = BillingMode.PROVISIONED;

            await installer.CreateTable(tableConfiguration, CancellationToken.None);

            var table = await dynamoClient.DescribeTableAsync(tableConfiguration.TableName);

            // Don't assert on BillingModeSummary as it may be not set when using provisioned mode.
            Assert.AreEqual(tableConfiguration.ProvisionedThroughput.ReadCapacityUnits, table.Table.ProvisionedThroughput.ReadCapacityUnits);
            Assert.AreEqual(tableConfiguration.ProvisionedThroughput.WriteCapacityUnits, table.Table.ProvisionedThroughput.WriteCapacityUnits);
        }

        [Test]
        public async Task Should_not_throw_when_same_table_already_exists()
        {
            tableConfiguration.PartitionKeyName = "PK1";
            await installer.CreateTable(tableConfiguration, CancellationToken.None);

            var table1 = await dynamoClient.DescribeTableAsync(tableConfiguration.TableName);

            tableConfiguration.PartitionKeyName = "PK2";
            await installer.CreateTable(tableConfiguration, CancellationToken.None);
            var table2 = await dynamoClient.DescribeTableAsync(tableConfiguration.TableName);

            Assert.AreEqual(table1.Table.CreationDateTime, table2.Table.CreationDateTime);
            Assert.AreEqual("PK1", table1.Table.KeySchema[0].AttributeName);
            Assert.AreEqual("PK1", table2.Table.KeySchema[0].AttributeName);
        }

        [Test]
        public async Task Should_configure_time_to_live_when_set()
        {
            tableConfiguration.TimeToLiveAttributeName = Guid.NewGuid().ToString("N");

            await installer.CreateTable(tableConfiguration, CancellationToken.None);

            var ttlSettings = await dynamoClient.DescribeTimeToLiveAsync(tableConfiguration.TableName);
            Assert.AreEqual(TimeToLiveStatus.ENABLED, ttlSettings.TimeToLiveDescription.TimeToLiveStatus);
            Assert.AreEqual(tableConfiguration.TimeToLiveAttributeName, ttlSettings.TimeToLiveDescription.AttributeName);
        }

        [Test]
        public async Task Should_throw_when_ttl_already_configured_for_different_attribute()
        {
            var existingTtlAttribute = Guid.NewGuid().ToString("N");
            tableConfiguration.TimeToLiveAttributeName = existingTtlAttribute;
            await installer.CreateTable(tableConfiguration, CancellationToken.None);

            tableConfiguration.TimeToLiveAttributeName = Guid.NewGuid().ToString("N");
            var exception = Assert.ThrowsAsync<Exception>(() => installer.CreateTable(tableConfiguration, CancellationToken.None));

            StringAssert.Contains($"The table '{tableConfiguration.TableName}' has attribute '{existingTtlAttribute}' configured for the time to live.", exception.Message);
        }
    }
}