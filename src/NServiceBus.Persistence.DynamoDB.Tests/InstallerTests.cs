namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System;
    using System.Linq;
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
        InstallerSettings installerSettings;
        Installer installer;
        OutboxPersistenceConfiguration outboxSettings;
        SagaPersistenceConfiguration sagaSettings;

        [SetUp]
        public void Setup()
        {
            dynamoClient = new AmazonDynamoDBClient(new EnvironmentVariablesAWSCredentials(), new AmazonDynamoDBConfig());
            installerSettings = new InstallerSettings
            {
                CreateSagaTable = false,
                CreateOutboxTable = false,
            };
            outboxSettings = new OutboxPersistenceConfiguration()
            {
                TableName = Guid.NewGuid().ToString("N") + "_installer_tests_outbox",
                PartitionKeyName = Guid.NewGuid().ToString("N"),
                SortKeyName = Guid.NewGuid().ToString("N")
            };
            sagaSettings = new SagaPersistenceConfiguration()
            {
                TableName = Guid.NewGuid().ToString("N") + "_installer_tests_saga",
                PartitionKeyName = Guid.NewGuid().ToString("N"),
                SortKeyName = Guid.NewGuid().ToString("N")
            };
            installer =
                new Installer(
                    new DynamoDBClientProvidedByConfiguration
                    {
                        Client = dynamoClient
                    }, installerSettings, outboxSettings, sagaSettings);
        }

        [TearDown]
        public async Task Teardown()
        {
            try
            {
                if (installerSettings.CreateOutboxTable)
                {
                    await dynamoClient.DeleteTableAsync(outboxSettings.TableName);
                }

                if (installerSettings.CreateSagaTable)
                {
                    await dynamoClient.DeleteTableAsync(sagaSettings.TableName);
                }
            }
            catch (Exception e)
            {
                TestContext.WriteLine($"Error deleting queue: {e}");
            }
        }

        class SagaInstallationTests : InstallerTests
        {
            [SetUp]
            public void EnableSagaCreation()
            {
                installerSettings.CreateSagaTable = true;
            }

            [Test]
            public async Task Should_create_table_with_configured_attribute_names()
            {
                sagaSettings.PartitionKeyName = Guid.NewGuid().ToString("N");
                sagaSettings.SortKeyName = Guid.NewGuid().ToString("N");

                await installer.Install();

                var table = await dynamoClient.DescribeTableAsync(sagaSettings.TableName);

                Assert.AreEqual(sagaSettings.TableName, table.Table.TableName);
                Assert.AreEqual(sagaSettings.PartitionKeyName, table.Table.KeySchema[0].AttributeName);
                Assert.AreEqual(KeyType.HASH, table.Table.KeySchema[0].KeyType);
                Assert.AreEqual(sagaSettings.SortKeyName, table.Table.KeySchema[1].AttributeName);
                Assert.AreEqual(KeyType.RANGE, table.Table.KeySchema[1].KeyType);
                //TODO do we also need to test the attribute type to be "S"?
                Assert.AreEqual(TableStatus.ACTIVE, table.Table.TableStatus);
            }

            [Test]
            public async Task Should_create_table_with_pay_as_you_go_billing_mode()
            {
                installerSettings.BillingMode = BillingMode.PAY_PER_REQUEST;

                await installer.Install();

                var table = await dynamoClient.DescribeTableAsync(sagaSettings.TableName);

                Assert.AreEqual(BillingMode.PAY_PER_REQUEST, table.Table.BillingModeSummary.BillingMode);
                Assert.AreEqual(0, table.Table.ProvisionedThroughput.ReadCapacityUnits);
                Assert.AreEqual(0, table.Table.ProvisionedThroughput.WriteCapacityUnits);
            }

            [Ignore("need to figure out the cost impact of this test first")]
            [Test]
            public async Task Should_create_table_with_provisioned_billing_mode()
            {
                installerSettings.ProvisionedThroughput = new ProvisionedThroughput(1, 1);
                installerSettings.BillingMode = BillingMode.PROVISIONED;

                await installer.Install();

                var table = await dynamoClient.DescribeTableAsync(sagaSettings.TableName);

                Assert.IsNull(table.Table.BillingModeSummary.BillingMode); // value is null when using provisioned mode
                Assert.AreEqual(installerSettings.ProvisionedThroughput.ReadCapacityUnits, table.Table.ProvisionedThroughput.ReadCapacityUnits);
                Assert.AreEqual(installerSettings.ProvisionedThroughput.WriteCapacityUnits, table.Table.ProvisionedThroughput.WriteCapacityUnits);
            }

            [Test]
            public async Task Should_not_throw_when_same_table_already_exists()
            {
                sagaSettings.PartitionKeyName = "PK1";
                await installer.Install();

                var table1 = await dynamoClient.DescribeTableAsync(sagaSettings.TableName);

                sagaSettings.PartitionKeyName = "PK2";
                await installer.Install();
                var table2 = await dynamoClient.DescribeTableAsync(sagaSettings.TableName);

                Assert.AreEqual(table1.Table.CreationDateTime, table2.Table.CreationDateTime);
                Assert.AreEqual("PK1", table1.Table.KeySchema[0].AttributeName);
                Assert.AreEqual("PK1", table2.Table.KeySchema[0].AttributeName);
            }

            [Test]
            public async Task Should_not_create_table_when_outbox_creation_disabled()
            {
                installerSettings.CreateSagaTable = false;

                await installer.Install();

                Assert.ThrowsAsync<ResourceNotFoundException>(() => dynamoClient.DescribeTableAsync(sagaSettings.TableName));
            }
        }

        class OutboxInstallationTests : InstallerTests
        {
            [SetUp]
            public void EnableOutboxCreation()
            {
                installerSettings.CreateOutboxTable = true;
            }

            [Test]
            public async Task Should_create_table_with_configured_attribute_names()
            {
                outboxSettings.PartitionKeyName = Guid.NewGuid().ToString("N");
                outboxSettings.SortKeyName = Guid.NewGuid().ToString("N");

                await installer.Install();

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
                installerSettings.BillingMode = BillingMode.PAY_PER_REQUEST;

                await installer.Install();

                var table = await dynamoClient.DescribeTableAsync(outboxSettings.TableName);

                Assert.AreEqual(BillingMode.PAY_PER_REQUEST, table.Table.BillingModeSummary.BillingMode);
                Assert.AreEqual(0, table.Table.ProvisionedThroughput.ReadCapacityUnits);
                Assert.AreEqual(0, table.Table.ProvisionedThroughput.WriteCapacityUnits);
            }

            [Ignore("need to figure out the cost impact of this test first")]
            [Test]
            public async Task Should_create_table_with_provisioned_billing_mode()
            {
                installerSettings.ProvisionedThroughput = new ProvisionedThroughput(1, 1);
                installerSettings.BillingMode = BillingMode.PROVISIONED;

                await installer.Install();

                var table = await dynamoClient.DescribeTableAsync(outboxSettings.TableName);

                Assert.IsNull(table.Table.BillingModeSummary.BillingMode); // value is null when using provisioned mode
                Assert.AreEqual(installerSettings.ProvisionedThroughput.ReadCapacityUnits, table.Table.ProvisionedThroughput.ReadCapacityUnits);
                Assert.AreEqual(installerSettings.ProvisionedThroughput.WriteCapacityUnits, table.Table.ProvisionedThroughput.WriteCapacityUnits);
            }

            [Test]
            public async Task Should_not_throw_when_same_table_already_exists()
            {
                outboxSettings.PartitionKeyName = "PK1";
                await installer.Install();

                var table1 = await dynamoClient.DescribeTableAsync(outboxSettings.TableName);

                outboxSettings.PartitionKeyName = "PK2";
                await installer.Install();
                var table2 = await dynamoClient.DescribeTableAsync(outboxSettings.TableName);

                Assert.AreEqual(table1.Table.CreationDateTime, table2.Table.CreationDateTime);
                Assert.AreEqual("PK1", table1.Table.KeySchema[0].AttributeName);
                Assert.AreEqual("PK1", table2.Table.KeySchema[0].AttributeName);
            }

            [Test]
            public async Task Should_not_create_table_when_outbox_creation_disabled()
            {
                installerSettings.CreateOutboxTable = false;

                await installer.Install();

                Assert.ThrowsAsync<ResourceNotFoundException>(() => dynamoClient.DescribeTableAsync(outboxSettings.TableName));
            }

            [Test]
            public async Task Should_configure_time_to_live_when_set()
            {
                outboxSettings.TimeToLiveAttributeName = Guid.NewGuid().ToString("N");

                await installer.Install();

                var ttlSettings = await dynamoClient.DescribeTimeToLiveAsync(outboxSettings.TableName);
                Assert.AreEqual(TimeToLiveStatus.ENABLED, ttlSettings.TimeToLiveDescription.TimeToLiveStatus);
                Assert.AreEqual(outboxSettings.TimeToLiveAttributeName, ttlSettings.TimeToLiveDescription.AttributeName);
            }

            [Test]
            public async Task Should_throw_when_ttl_already_configured_for_different_attribute()
            {
                var existingTtlAttribute = Guid.NewGuid().ToString("N");
                outboxSettings.TimeToLiveAttributeName = existingTtlAttribute;
                await installer.Install();

                outboxSettings.TimeToLiveAttributeName = Guid.NewGuid().ToString("N");
                var exception = Assert.ThrowsAsync<Exception>(() => installer.Install());

                StringAssert.Contains($"The table {outboxSettings.TableName} has attribute {existingTtlAttribute} configured for the time to live.", exception.Message);
            }
        }
    }
}