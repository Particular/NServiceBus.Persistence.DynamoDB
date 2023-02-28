namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System;
    using System.Linq;
    using System.Threading;
    using Amazon.DynamoDBv2.Model;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using NUnit.Framework;
    using Amazon.Runtime;

    //TODO test when both saga+outbox are enabled
    //TODO would be nice to be able to tag the tables as tests for easier identification in AWS
    [TestFixture]
    public abstract class InstallerTests
    {
        AmazonDynamoDBClient dynamoClient;
        Installer installer;
        OutboxPersistenceConfiguration outboxSettings;
        SagaPersistenceConfiguration sagaSettings;

        [SetUp]
        public void Setup()
        {
            dynamoClient = new AmazonDynamoDBClient(new EnvironmentVariablesAWSCredentials(), new AmazonDynamoDBConfig());
            outboxSettings = new OutboxPersistenceConfiguration
            {
                TableName = Guid.NewGuid().ToString("N") + "_installer_tests_outbox",
                PartitionKeyName = Guid.NewGuid().ToString("N"),
                SortKeyName = Guid.NewGuid().ToString("N")
            };
            sagaSettings = new SagaPersistenceConfiguration
            {
                TableName = Guid.NewGuid().ToString("N") + "_installer_tests_saga",
                PartitionKeyName = Guid.NewGuid().ToString("N"),
                SortKeyName = Guid.NewGuid().ToString("N")
            };
            installer = new Installer(dynamoClient);
        }

        class SagaInstallationTests : InstallerTests
        {
            [TearDown]
            public async Task Teardown()
            {
                try
                {
                    await dynamoClient.DeleteTableAsync(sagaSettings.TableName);
                }
                catch (Exception e)
                {
                    TestContext.WriteLine($"Error deleting queue: {e}");
                }
            }

            [Test]
            public async Task Should_create_table_with_configured_attribute_names()
            {
                sagaSettings.PartitionKeyName = Guid.NewGuid().ToString("N");
                sagaSettings.SortKeyName = Guid.NewGuid().ToString("N");

                await installer.CreateSagaTableIfNotExists(sagaSettings, CancellationToken.None);

                var table = await dynamoClient.DescribeTableAsync(sagaSettings.TableName);

                Assert.AreEqual(sagaSettings.TableName, table.Table.TableName);
                Assert.AreEqual(sagaSettings.PartitionKeyName, table.Table.KeySchema[0].AttributeName);
                Assert.AreEqual(KeyType.HASH, table.Table.KeySchema[0].KeyType);
                Assert.AreEqual(ScalarAttributeType.S, table.Table.AttributeDefinitions.Single(a => a.AttributeName == outboxSettings.PartitionKeyName).AttributeType);
                Assert.AreEqual(sagaSettings.SortKeyName, table.Table.KeySchema[1].AttributeName);
                Assert.AreEqual(KeyType.RANGE, table.Table.KeySchema[1].KeyType);
                Assert.AreEqual(ScalarAttributeType.S, table.Table.AttributeDefinitions.Single(a => a.AttributeName == outboxSettings.SortKeyName).AttributeType);
                Assert.AreEqual(TableStatus.ACTIVE, table.Table.TableStatus);
            }

            [Test]
            public async Task Should_create_table_with_pay_as_you_go_billing_mode()
            {
                sagaSettings.BillingMode = BillingMode.PAY_PER_REQUEST;

                await installer.CreateSagaTableIfNotExists(sagaSettings, CancellationToken.None);

                var table = await dynamoClient.DescribeTableAsync(sagaSettings.TableName);

                Assert.AreEqual(BillingMode.PAY_PER_REQUEST, table.Table.BillingModeSummary.BillingMode);
                Assert.AreEqual(0, table.Table.ProvisionedThroughput.ReadCapacityUnits);
                Assert.AreEqual(0, table.Table.ProvisionedThroughput.WriteCapacityUnits);
            }

            [Ignore("need to figure out the cost impact of this test first")]
            [Test]
            public async Task Should_create_table_with_provisioned_billing_mode()
            {
                sagaSettings.ProvisionedThroughput = new ProvisionedThroughput(1, 1);
                sagaSettings.BillingMode = BillingMode.PROVISIONED;

                await installer.CreateSagaTableIfNotExists(sagaSettings, CancellationToken.None);

                var table = await dynamoClient.DescribeTableAsync(sagaSettings.TableName);

                Assert.IsNull(table.Table.BillingModeSummary.BillingMode); // value is null when using provisioned mode
                Assert.AreEqual(sagaSettings.ProvisionedThroughput.ReadCapacityUnits, table.Table.ProvisionedThroughput.ReadCapacityUnits);
                Assert.AreEqual(sagaSettings.ProvisionedThroughput.WriteCapacityUnits, table.Table.ProvisionedThroughput.WriteCapacityUnits);
            }

            [Test]
            public async Task Should_not_throw_when_same_table_already_exists()
            {
                sagaSettings.PartitionKeyName = "PK1";
                await installer.CreateSagaTableIfNotExists(sagaSettings, CancellationToken.None);

                var table1 = await dynamoClient.DescribeTableAsync(sagaSettings.TableName);

                sagaSettings.PartitionKeyName = "PK2";
                await installer.CreateSagaTableIfNotExists(sagaSettings, CancellationToken.None);
                var table2 = await dynamoClient.DescribeTableAsync(sagaSettings.TableName);

                Assert.AreEqual(table1.Table.CreationDateTime, table2.Table.CreationDateTime);
                Assert.AreEqual("PK1", table1.Table.KeySchema[0].AttributeName);
                Assert.AreEqual("PK1", table2.Table.KeySchema[0].AttributeName);
            }
        }

        class OutboxInstallationTests : InstallerTests
        {
            [TearDown]
            public async Task Teardown()
            {
                try
                {
                    await dynamoClient.DeleteTableAsync(outboxSettings.TableName);
                }
                catch (Exception e)
                {
                    TestContext.WriteLine($"Error deleting queue: {e}");
                }
            }

            [Test]
            public async Task Should_create_table_with_configured_attribute_names()
            {
                outboxSettings.PartitionKeyName = Guid.NewGuid().ToString("N");
                outboxSettings.SortKeyName = Guid.NewGuid().ToString("N");

                await installer.CreateOutboxTableIfNotExists(outboxSettings, CancellationToken.None);

                var table = await dynamoClient.DescribeTableAsync(outboxSettings.TableName);

                Assert.AreEqual(outboxSettings.TableName, table.Table.TableName);
                Assert.AreEqual(outboxSettings.PartitionKeyName, table.Table.KeySchema[0].AttributeName);
                Assert.AreEqual(KeyType.HASH, table.Table.KeySchema[0].KeyType);
                Assert.AreEqual(ScalarAttributeType.S, table.Table.AttributeDefinitions.Single(a => a.AttributeName == outboxSettings.PartitionKeyName).AttributeType);
                Assert.AreEqual(outboxSettings.SortKeyName, table.Table.KeySchema[1].AttributeName);
                Assert.AreEqual(KeyType.RANGE, table.Table.KeySchema[1].KeyType);
                Assert.AreEqual(ScalarAttributeType.S, table.Table.AttributeDefinitions.Single(a => a.AttributeName == outboxSettings.SortKeyName).AttributeType);
                Assert.AreEqual(TableStatus.ACTIVE, table.Table.TableStatus);
            }

            [Test]
            public async Task Should_create_table_with_pay_as_you_go_billing_mode()
            {
                outboxSettings.BillingMode = BillingMode.PAY_PER_REQUEST;

                await installer.CreateOutboxTableIfNotExists(outboxSettings, CancellationToken.None);

                var table = await dynamoClient.DescribeTableAsync(outboxSettings.TableName);

                Assert.AreEqual(BillingMode.PAY_PER_REQUEST, table.Table.BillingModeSummary.BillingMode);
                Assert.AreEqual(0, table.Table.ProvisionedThroughput.ReadCapacityUnits);
                Assert.AreEqual(0, table.Table.ProvisionedThroughput.WriteCapacityUnits);
            }

            [Ignore("need to figure out the cost impact of this test first")]
            [Test]
            public async Task Should_create_table_with_provisioned_billing_mode()
            {
                outboxSettings.ProvisionedThroughput = new ProvisionedThroughput(1, 1);
                outboxSettings.BillingMode = BillingMode.PROVISIONED;

                await installer.CreateOutboxTableIfNotExists(outboxSettings, CancellationToken.None);

                var table = await dynamoClient.DescribeTableAsync(outboxSettings.TableName);

                Assert.IsNull(table.Table.BillingModeSummary.BillingMode); // value is null when using provisioned mode
                Assert.AreEqual(outboxSettings.ProvisionedThroughput.ReadCapacityUnits, table.Table.ProvisionedThroughput.ReadCapacityUnits);
                Assert.AreEqual(outboxSettings.ProvisionedThroughput.WriteCapacityUnits, table.Table.ProvisionedThroughput.WriteCapacityUnits);
            }

            [Test]
            public async Task Should_not_throw_when_same_table_already_exists()
            {
                outboxSettings.PartitionKeyName = "PK1";
                await installer.CreateOutboxTableIfNotExists(outboxSettings, CancellationToken.None);

                var table1 = await dynamoClient.DescribeTableAsync(outboxSettings.TableName);

                outboxSettings.PartitionKeyName = "PK2";
                await installer.CreateOutboxTableIfNotExists(outboxSettings, CancellationToken.None);
                var table2 = await dynamoClient.DescribeTableAsync(outboxSettings.TableName);

                Assert.AreEqual(table1.Table.CreationDateTime, table2.Table.CreationDateTime);
                Assert.AreEqual("PK1", table1.Table.KeySchema[0].AttributeName);
                Assert.AreEqual("PK1", table2.Table.KeySchema[0].AttributeName);
            }

            [Test]
            public async Task Should_configure_time_to_live_when_set()
            {
                outboxSettings.TimeToLiveAttributeName = Guid.NewGuid().ToString("N");

                await installer.CreateOutboxTableIfNotExists(outboxSettings, CancellationToken.None);

                var ttlSettings = await dynamoClient.DescribeTimeToLiveAsync(outboxSettings.TableName);
                Assert.AreEqual(TimeToLiveStatus.ENABLED, ttlSettings.TimeToLiveDescription.TimeToLiveStatus);
                Assert.AreEqual(outboxSettings.TimeToLiveAttributeName, ttlSettings.TimeToLiveDescription.AttributeName);
            }

            [Test]
            public async Task Should_throw_when_ttl_already_configured_for_different_attribute()
            {
                var existingTtlAttribute = Guid.NewGuid().ToString("N");
                outboxSettings.TimeToLiveAttributeName = existingTtlAttribute;
                await installer.CreateOutboxTableIfNotExists(outboxSettings, CancellationToken.None);

                outboxSettings.TimeToLiveAttributeName = Guid.NewGuid().ToString("N");
                var exception = Assert.ThrowsAsync<Exception>(() => installer.CreateOutboxTableIfNotExists(outboxSettings, CancellationToken.None));

                StringAssert.Contains($"The table {outboxSettings.TableName} has attribute {existingTtlAttribute} configured for the time to live.", exception.Message);
            }
        }
    }
}