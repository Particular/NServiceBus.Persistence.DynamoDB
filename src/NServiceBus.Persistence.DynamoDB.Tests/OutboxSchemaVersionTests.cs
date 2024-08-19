namespace NServiceBus.Persistence.DynamoDB.Tests;

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Extensibility;
using NUnit.Framework;
using Outbox;
using Particular.Approvals;

[TestFixture]
public class OutboxSchemaVersionTests
{
    [Test]
    public async Task Should_update_schema_version_on_schema_changes()
    {
        var client = new MockDynamoDBClient();
        var outboxPersister = new OutboxPersister(client, new OutboxPersistenceConfiguration(), "SchemaVersionTest");

        var outboxMessage = new OutboxMessage("FFC8A2FD-0335-47C8-A29D-9EEA6C8445D8", Array.Empty<TransportOperation>());

        var context = new ContextBag();

        using var dynamoDbOutboxTransaction = new DynamoOutboxTransaction(client, context);
        using var outboxTransaction = await outboxPersister.BeginTransaction(context);
        await outboxPersister.Store(outboxMessage, dynamoDbOutboxTransaction, context);
        await outboxTransaction.Commit();
        await dynamoDbOutboxTransaction.Commit();

        // !!! IMPORTANT !!!
        // This test should help to detect data schema changes.
        // When this test fails, make sure to update the outbox data schema version in the metadata.
        Approver.Verify(
            JsonSerializer.Serialize(client.TransactWriteRequestsSent.Single().TransactItems,
                new JsonSerializerOptions { WriteIndented = true }));
    }

    [Theory]
    [TestCase("SchemaVersionTest", "SomeMessageId")]
    [TestCase("schemaversiontest", "somemessageid")]
    [TestCase("SCHEMAVERSIONTEST", "SOMEMESSAGEID")]
    public async Task Should_treat_identifier_and_message_id_case_sensitive(string endpointIdentifier, string messageId)
    {
        var client = new MockDynamoDBClient();
        var outboxPersister = new OutboxPersister(client, new OutboxPersistenceConfiguration(), endpointIdentifier);

        var outboxMessage = new OutboxMessage(messageId, Array.Empty<TransportOperation>());

        var context = new ContextBag();

        using var dynamoDbOutboxTransaction = new DynamoOutboxTransaction(client, context);
        using var outboxTransaction = await outboxPersister.BeginTransaction(context);
        await outboxPersister.Store(outboxMessage, dynamoDbOutboxTransaction, context);
        await outboxTransaction.Commit();
        await dynamoDbOutboxTransaction.Commit();

        var put = client.TransactWriteRequestsSent.Single().TransactItems[0].Put;

        Assert.That(put.Item["PK"].S, Does.Contain(endpointIdentifier));
        Assert.That(put.Item["PK"].S, Does.Contain(messageId));

        Assert.That(put.Item["SK"].S, Does.Contain(messageId));
    }
}