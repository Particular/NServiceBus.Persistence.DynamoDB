namespace NServiceBus.Persistence.DynamoDB.Tests;

using System.Threading.Tasks;
using Amazon.Runtime;
using NServiceBus.Extensibility;
using NUnit.Framework;

[TestFixture]
public class OutboxPersisterTests
{
    [SetUp]
    public void SetUp()
    {
        client = new MockDynamoDBClient();

        persister = new OutboxPersister(client, new OutboxPersistenceConfiguration(), "endpointIdentifier");
    }

    [Test]
    public async Task Should_update_metadata_as_a_dedicated_non_batched_update()
    {
        var contextBag = new ContextBag();
        contextBag.Set("dynamo_operations_count:someMessageId", 10);

        await persister.SetAsDispatched("someMessageId", contextBag);

        Assert.That(client.UpdateItemRequestsSent, Has.Count.EqualTo(1));
    }

    [Test]
    public void Should_not_execute_batched_operations_when_metadata_cannot_be_updated()
    {
        var contextBag = new ContextBag();
        contextBag.Set("dynamo_operations_count:someMessageId", 10);

        client.UpdateItemRequestResponse = _ => throw new AmazonClientException("");

        Assert.ThrowsAsync<AmazonClientException>(async () => await persister.SetAsDispatched("someMessageId", contextBag));
        Assert.That(client.BatchWriteRequestsSent, Has.Count.EqualTo(0));
    }

    [Test]
    public async Task Should_execute_batched_operations()
    {
        var contextBag = new ContextBag();
        contextBag.Set("dynamo_operations_count:someMessageId", 50);

        await persister.SetAsDispatched("someMessageId", contextBag);

        Assert.That(client.BatchWriteRequestsSent, Has.Count.EqualTo(2));
    }

    MockDynamoDBClient client;
    OutboxPersister persister;
}