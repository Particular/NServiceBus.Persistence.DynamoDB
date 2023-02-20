using System;
using Amazon.DynamoDBv2.Model;

namespace NServiceBus.Persistence.DynamoDB.Tests;

using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using NUnit.Framework;

[TestFixture]
public class InstallerTests
{
    AmazonDynamoDBClient dynamoClient;
    InstallerSettings installerSettings;
    Installer installer;

    [SetUp]
    public void Setup()
    {
        dynamoClient = new AmazonDynamoDBClient(new AmazonDynamoDBConfig()
        {
            ServiceURL = "http://localhost:8000"
        });
        installerSettings = new InstallerSettings()
        {
            CreateOutboxTable = true,
            OutboxTableName = Guid.NewGuid().ToString("N"),
            OutboxPartitionKeyName = Guid.NewGuid().ToString("N"),
            OutboxSortKeyName = Guid.NewGuid().ToString("N")
        };
        installer =
            new Installer(
                new DynamoDBClientProvidedByConfiguration
                {
                    Client = dynamoClient
                }, installerSettings);
    }

    [TearDown]
    public async Task Teardown()
    {
        try
        {
            await dynamoClient.DeleteTableAsync(installerSettings.OutboxTableName);
        }
        catch (Exception e)
        {
            TestContext.WriteLine($"Error deleting queue: {e}");
        }
    }

    [Test]
    public async Task Should_create_outbox_with_configured_attribute_names()
    {
        installerSettings.OutboxPartitionKeyName = Guid.NewGuid().ToString("N");
        installerSettings.OutboxSortKeyName = Guid.NewGuid().ToString("N");

        await installer.Install("");

        var table = await dynamoClient.DescribeTableAsync(installerSettings.OutboxTableName);

        Assert.AreEqual(installerSettings.OutboxTableName, table.Table.TableName);
        Assert.AreEqual(installerSettings.OutboxPartitionKeyName, table.Table.KeySchema[0].AttributeName);
        Assert.AreEqual(KeyType.HASH, table.Table.KeySchema[0].KeyType);
        Assert.AreEqual(installerSettings.OutboxSortKeyName, table.Table.KeySchema[1].AttributeName);
        Assert.AreEqual(KeyType.RANGE, table.Table.KeySchema[1].KeyType);
        //TODO do we also need to test the attribute type to be "S"?
        Assert.AreEqual(TableStatus.ACTIVE, table.Table.TableStatus);
    }

    [Test]
    public async Task Should_create_outbox_with_pay_as_you_go_billing_mode()
    {
        installerSettings.BillingMode = BillingMode.PAY_PER_REQUEST;

        await installer.Install("");

        var table = await dynamoClient.DescribeTableAsync(installerSettings.OutboxTableName);

        Assert.AreEqual(BillingMode.PAY_PER_REQUEST, table.Table.BillingModeSummary.BillingMode);
        Assert.AreEqual(0, table.Table.ProvisionedThroughput.ReadCapacityUnits);
        Assert.AreEqual(0, table.Table.ProvisionedThroughput.WriteCapacityUnits);
    }

    [Test]
    //TODO this doesn't work with the local DB because BillingModeSummary will be null
    //TODO what's the cost impact on this test?
    public async Task Should_create_outbox_with_provisioned_billing_mode()
    {
        installerSettings.ProvisionedThroughput = new ProvisionedThroughput(1, 1);
        installerSettings.BillingMode = BillingMode.PROVISIONED;

        await installer.Install("");

        var table = await dynamoClient.DescribeTableAsync(installerSettings.OutboxTableName);

        Assert.AreEqual(BillingMode.PROVISIONED, table.Table.BillingModeSummary.BillingMode);
        Assert.AreEqual(installerSettings.ProvisionedThroughput.ReadCapacityUnits, table.Table.ProvisionedThroughput.ReadCapacityUnits);
        Assert.AreEqual(installerSettings.ProvisionedThroughput.WriteCapacityUnits, table.Table.ProvisionedThroughput.WriteCapacityUnits);
    }

    [Test]
    public async Task Should_not_throw_when_same_table_already_exists()
    {
        installerSettings.OutboxPartitionKeyName = "PK1";
        await installer.Install("");

        var table1 = await dynamoClient.DescribeTableAsync(installerSettings.OutboxTableName);

        installerSettings.OutboxPartitionKeyName = "PK2";
        await installer.Install("");
        var table2 = await dynamoClient.DescribeTableAsync(installerSettings.OutboxTableName);

        Assert.AreEqual(table1.Table.CreationDateTime, table2.Table.CreationDateTime);
        Assert.AreEqual("PK1", table1.Table.KeySchema[0].AttributeName);
        Assert.AreEqual("PK1", table2.Table.KeySchema[0].AttributeName);
    }

    [Test]
    public async Task Should_not_create_table_when_outbox_creation_disabled()
    {
        installerSettings.CreateOutboxTable = false;

        var installer =
            new Installer(
                new DynamoDBClientProvidedByConfiguration
                {
                    Client = dynamoClient
                }, installerSettings);

        await installer.Install("");

        Assert.ThrowsAsync<ResourceNotFoundException>(() => dynamoClient.DescribeTableAsync(installerSettings.OutboxTableName));
    }
}