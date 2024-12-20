namespace NServiceBus.Persistence.DynamoDB.Tests;

using System;
using System.Linq;
using System.Threading;
using Amazon.DynamoDBv2.Model;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using NUnit.Framework;

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
            dynamoClient.Dispose();
        }
        catch (Exception e)
        {
            await TestContext.Out.WriteLineAsync($"Error deleting queue: {e}");
        }
    }

    [Test]
    public async Task Should_create_table_with_configured_attribute_names()
    {
        tableConfiguration.PartitionKeyName = Guid.NewGuid().ToString("N");
        tableConfiguration.SortKeyName = Guid.NewGuid().ToString("N");

        await installer.CreateTable(tableConfiguration, CancellationToken.None);

        var table = await dynamoClient.DescribeTableAsync(tableConfiguration.TableName);

        Assert.Multiple(() =>
        {
            Assert.That(table.Table.TableName, Is.EqualTo(tableConfiguration.TableName));
            Assert.That(table.Table.KeySchema[0].AttributeName, Is.EqualTo(tableConfiguration.PartitionKeyName));
            Assert.That(table.Table.KeySchema[0].KeyType.Value, Is.EqualTo(KeyType.HASH.Value));
            Assert.That(table.Table.AttributeDefinitions.Single(a => a.AttributeName == tableConfiguration.PartitionKeyName).AttributeType.Value, Is.EqualTo(ScalarAttributeType.S.Value));
            Assert.That(table.Table.KeySchema[1].AttributeName, Is.EqualTo(tableConfiguration.SortKeyName));
            Assert.That(table.Table.KeySchema[1].KeyType.Value, Is.EqualTo(KeyType.RANGE.Value));
            Assert.That(table.Table.AttributeDefinitions.Single(a => a.AttributeName == tableConfiguration.SortKeyName).AttributeType.Value, Is.EqualTo(ScalarAttributeType.S.Value));
            Assert.That(table.Table.TableStatus.Value, Is.EqualTo(TableStatus.ACTIVE.Value));
        });
    }

    [Test]
    public async Task Should_create_table_with_pay_as_you_go_billing_mode()
    {
        tableConfiguration.BillingMode = BillingMode.PAY_PER_REQUEST;

        await installer.CreateTable(tableConfiguration, CancellationToken.None);

        var table = await dynamoClient.DescribeTableAsync(tableConfiguration.TableName);

        Assert.Multiple(() =>
        {
            Assert.That(table.Table.BillingModeSummary.BillingMode.Value, Is.EqualTo(BillingMode.PAY_PER_REQUEST.Value));
            Assert.That(table.Table.ProvisionedThroughput.ReadCapacityUnits, Is.EqualTo(0));
            Assert.That(table.Table.ProvisionedThroughput.WriteCapacityUnits, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task Should_create_table_with_provisioned_billing_mode()
    {
        tableConfiguration.ProvisionedThroughput = new ProvisionedThroughput(1, 1);
        tableConfiguration.BillingMode = BillingMode.PROVISIONED;

        await installer.CreateTable(tableConfiguration, CancellationToken.None);

        var table = await dynamoClient.DescribeTableAsync(tableConfiguration.TableName);

        Assert.Multiple(() =>
        {
            // Don't assert on BillingModeSummary as it may be not set when using provisioned mode.
            Assert.That(table.Table.ProvisionedThroughput.ReadCapacityUnits, Is.EqualTo(tableConfiguration.ProvisionedThroughput.ReadCapacityUnits));
            Assert.That(table.Table.ProvisionedThroughput.WriteCapacityUnits, Is.EqualTo(tableConfiguration.ProvisionedThroughput.WriteCapacityUnits));
        });
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

        Assert.Multiple(() =>
        {
            Assert.That(table2.Table.CreationDateTime, Is.EqualTo(table1.Table.CreationDateTime));
            Assert.That(table1.Table.KeySchema[0].AttributeName, Is.EqualTo("PK1"));
            Assert.That(table2.Table.KeySchema[0].AttributeName, Is.EqualTo("PK1"));
        });
    }

    [Test]
    public async Task Should_configure_time_to_live_when_set()
    {
        tableConfiguration.TimeToLiveAttributeName = Guid.NewGuid().ToString("N");

        await installer.CreateTable(tableConfiguration, CancellationToken.None);

        var ttlSettings = await dynamoClient.DescribeTimeToLiveAsync(tableConfiguration.TableName);
        Assert.Multiple(() =>
        {
            Assert.That(ttlSettings.TimeToLiveDescription.TimeToLiveStatus.Value, Is.EqualTo(TimeToLiveStatus.ENABLED.Value));
            Assert.That(ttlSettings.TimeToLiveDescription.AttributeName, Is.EqualTo(tableConfiguration.TimeToLiveAttributeName));
        });
    }

    [Test]
    public async Task Should_throw_when_ttl_already_configured_for_different_attribute()
    {
        var existingTtlAttribute = Guid.NewGuid().ToString("N");
        tableConfiguration.TimeToLiveAttributeName = existingTtlAttribute;
        await installer.CreateTable(tableConfiguration, CancellationToken.None);

        tableConfiguration.TimeToLiveAttributeName = Guid.NewGuid().ToString("N");
        var exception = Assert.ThrowsAsync<Exception>(() => installer.CreateTable(tableConfiguration, CancellationToken.None));

        Assert.That(exception.Message, Does.Contain($"The table '{tableConfiguration.TableName}' has attribute '{existingTtlAttribute}' configured for the time to live."));
    }
}