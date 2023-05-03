namespace NServiceBus.PersistenceTesting;

using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Extensibility;
using NServiceBus.Outbox;
using NUnit.Framework;
using Transport;
using TransportOperation = NServiceBus.Outbox.TransportOperation;

[TestFixtureSource(typeof(PersistenceTestsConfiguration), nameof(PersistenceTestsConfiguration.OutboxVariants))]
public class OutboxTests
{
    readonly TestVariant param;
    PersistenceTestsConfiguration configuration;

    public OutboxTests(TestVariant param)
    {
        this.param = param.DeepCopy();
    }

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        configuration = new PersistenceTestsConfiguration(param);
        await configuration.Configure();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await configuration.Cleanup();
    }

    [Test]
    public async Task Should_fail_when_outbox_item_beyond_size_limit()
    {
        ContextBag contextBag = configuration.GetContextBagForOutbox();

        var r = new Random();
        byte[] payload = new byte[500_000]; // 400 KB is the item size limit
        r.NextBytes(payload);

        var transportOperations = new TransportOperation[]
        {
            new(Guid.NewGuid().ToString(), new DispatchProperties(),
                payload, new Dictionary<string, string>())
        };

        using var transaction = await configuration.OutboxStorage.BeginTransaction(contextBag);
        var outboxMessage = new OutboxMessage(Guid.NewGuid().ToString(), transportOperations);
        await configuration.OutboxStorage.Store(outboxMessage, transaction, contextBag);

        Assert.ThrowsAsync<AmazonDynamoDBException>(() => transaction.Commit());
    }

    [Test]
    public async Task Should_fail_when_outbox_operations_beyond_size_limit()
    {
        ContextBag contextBag = configuration.GetContextBagForOutbox();

        var r = new Random();
        byte[] payload = new byte[300_000]; // 400 KB is the item size limit
        r.NextBytes(payload);

        // Generate total transaction payload > 4MB
        var transportOperations = new TransportOperation[15];
        for (int i = 0; i < transportOperations.Length; i++)
        {
            transportOperations[i] = new TransportOperation(Guid.NewGuid().ToString(), new DispatchProperties(),
                payload, new Dictionary<string, string>());
        }

        using var transaction = await configuration.OutboxStorage.BeginTransaction(contextBag);
        var outboxMessage = new OutboxMessage(Guid.NewGuid().ToString(), transportOperations);
        await configuration.OutboxStorage.Store(outboxMessage, transaction, contextBag);

        Assert.ThrowsAsync<AmazonDynamoDBException>(() => transaction.Commit());
    }

    [Test]
    public async Task Should_fail_when_outbox_operations_beyond_transaction_item_limit()
    {
        ContextBag contextBag = configuration.GetContextBagForOutbox();

        var transportOperations = new TransportOperation[100]; // 100 items is the transaction limit, we generate n+1 items in the persister
        for (int i = 0; i < transportOperations.Length; i++)
        {
            transportOperations[i] = new TransportOperation(Guid.NewGuid().ToString(), new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>());
        }

        using var transaction = await configuration.OutboxStorage.BeginTransaction(contextBag);
        var outboxMessage = new OutboxMessage(Guid.NewGuid().ToString(), transportOperations);

        Assert.ThrowsAsync<AmazonDynamoDBException>(() => configuration.OutboxStorage.Store(outboxMessage, transaction, contextBag));

        // already throws at store, no need to commit
    }

    [Test]
    public async Task Should_fetch_complete_outbox_message_when_below_transaction_limit()
    {
        var incomingMessageId = Guid.NewGuid().ToString();
        ContextBag contextBag = configuration.GetContextBagForOutbox();

        var transportOperations = new TransportOperation[99]; // 100 items is the transaction limit, we generate n+1 items in the persister
        for (int i = 0; i < transportOperations.Length; i++)
        {
            transportOperations[i] = new TransportOperation(Guid.NewGuid().ToString(), new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>());
        }

        using (var transaction = await configuration.OutboxStorage.BeginTransaction(contextBag))
        {
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, contextBag);

            await transaction.Commit();
        }

        var result = await configuration.OutboxStorage.Get(incomingMessageId, configuration.GetContextBagForOutbox());
        Assert.AreEqual(transportOperations.Length, result.TransportOperations.Length);
    }

    [Test]
    public async Task Should_fetch_complete_outbox_message_when_total_size_beyond_single_query_limit()
    {
        var incomingMessageId = Guid.NewGuid().ToString();
        ContextBag contextBag = configuration.GetContextBagForOutbox();

        var r = new Random();
        byte[] payload = new byte[100_000]; // 400 KB is the item size limit
        r.NextBytes(payload);

        // Query size limit is 1MB
        var transportOperations = new TransportOperation[20];
        for (int i = 0; i < transportOperations.Length; i++)
        {
            transportOperations[i] = new TransportOperation(Guid.NewGuid().ToString(), new DispatchProperties(),
                payload, new Dictionary<string, string>());
        }

        using (var transaction = await configuration.OutboxStorage.BeginTransaction(contextBag))
        {
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, contextBag);

            await transaction.Commit();
        }

        var result = await configuration.OutboxStorage.Get(incomingMessageId, configuration.GetContextBagForOutbox());
        Assert.AreEqual(transportOperations.Length, result.TransportOperations.Length);
    }

    [Test]
    public async Task Should_delete_operations_when_set_as_dispatched()
    {
        var incomingMessageId = Guid.NewGuid().ToString();
        ContextBag contextBag = configuration.GetContextBagForOutbox();

        var transportOperations = new TransportOperation[24]; // 25 is the batch write limit
        for (int i = 0; i < transportOperations.Length; i++)
        {
            transportOperations[i] = new TransportOperation(Guid.NewGuid().ToString(), new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>());
        }

        using (var transaction = await configuration.OutboxStorage.BeginTransaction(contextBag))
        {
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, contextBag);

            await transaction.Commit();
        }

        await configuration.OutboxStorage.SetAsDispatched(incomingMessageId, contextBag);

        var result = await configuration.OutboxStorage.Get(incomingMessageId, configuration.GetContextBagForOutbox());
        Assert.AreEqual(0, result.TransportOperations.Length);
    }

    [Test]
    public async Task Should_not_fail_when_dispatching_multiple_times()
    {
        var incomingMessageId = Guid.NewGuid().ToString();
        ContextBag contextBag = configuration.GetContextBagForOutbox();

        var transportOperations = new TransportOperation[24]; // 25 is the batch write limit
        for (int i = 0; i < transportOperations.Length; i++)
        {
            transportOperations[i] = new TransportOperation(Guid.NewGuid().ToString(), new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>());
        }

        using (var transaction = await configuration.OutboxStorage.BeginTransaction(contextBag))
        {
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, contextBag);

            await transaction.Commit();
        }

        await configuration.OutboxStorage.SetAsDispatched(incomingMessageId, contextBag);

        var secondAttemptContextBag = configuration.GetContextBagForOutbox();
        var result1 = await configuration.OutboxStorage.Get(incomingMessageId, secondAttemptContextBag);
        await configuration.OutboxStorage.SetAsDispatched(incomingMessageId, secondAttemptContextBag);

        var result2 = await configuration.OutboxStorage.Get(incomingMessageId, configuration.GetContextBagForOutbox());
        Assert.AreEqual(0, result1.TransportOperations.Length);
        Assert.AreEqual(0, result2.TransportOperations.Length);
    }

    [Test]
    public async Task Should_delete_operations_when_set_as_dispatched_and_more_operations_than_max_batch_size()
    {
        var incomingMessageId = Guid.NewGuid().ToString();
        ContextBag contextBag = configuration.GetContextBagForOutbox();

        var transportOperations = new TransportOperation[99]; // 25 is the batch write limit and 100 the tx limit
        for (int i = 0; i < transportOperations.Length; i++)
        {
            transportOperations[i] = new TransportOperation(Guid.NewGuid().ToString(), new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>());
        }

        using (var transaction = await configuration.OutboxStorage.BeginTransaction(contextBag))
        {
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, contextBag);

            await transaction.Commit();
        }

        await configuration.OutboxStorage.SetAsDispatched(incomingMessageId, contextBag);

        var result = await configuration.OutboxStorage.Get(incomingMessageId, configuration.GetContextBagForOutbox());
        Assert.AreEqual(0, result.TransportOperations.Length);
    }

    [Test]
    public async Task Should_fail_on_multiple_stores_for_same_id_with_metadata_available()
    {
        var incomingMessageId = Guid.NewGuid().ToString();
        ContextBag fistAttemptContextBag = configuration.GetContextBagForOutbox();

        var transportOperations = new TransportOperation[]
        {
            new(Guid.NewGuid().ToString(), new DispatchProperties(),
                ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>())
        };

        using (var transaction = await configuration.OutboxStorage.BeginTransaction(fistAttemptContextBag))
        {
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, fistAttemptContextBag);

            await transaction.Commit();
        }

        Assert.ThrowsAsync<TransactionCanceledException>(async () =>
        {
            ContextBag secondAttemptContextBag = configuration.GetContextBagForOutbox();
            using var transaction = await configuration.OutboxStorage.BeginTransaction(secondAttemptContextBag);
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, secondAttemptContextBag);

            await transaction.Commit();
        });
    }

    [Test]
    public async Task Should_allow_multiple_stores_for_same_id_when_metadata_expired()
    {
        var incomingMessageId = Guid.NewGuid().ToString();
        ContextBag fistAttemptContextBag = configuration.GetContextBagForOutbox();

        var transportOperations = new TransportOperation[]
        {
            new(Guid.NewGuid().ToString(), new DispatchProperties(),
                ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>())
        };

        using (var transaction = await configuration.OutboxStorage.BeginTransaction(fistAttemptContextBag))
        {
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, fistAttemptContextBag);

            await transaction.Commit();
        }

        await ExpireMetadataRecord(incomingMessageId);

        ContextBag secondAttemptContextBag = configuration.GetContextBagForOutbox();
        using (var transaction = await configuration.OutboxStorage.BeginTransaction(secondAttemptContextBag))
        {
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, secondAttemptContextBag);

            await transaction.Commit();
        }

        var result = await configuration.OutboxStorage.Get(incomingMessageId, configuration.GetContextBagForOutbox());
        Assert.AreEqual(transportOperations.Length, result.TransportOperations.Length);
    }

    [Test]
    public async Task Should_return_fresh_entry_when_metadata_expired_but_phantom_record_present()
    {
        var incomingMessageId = Guid.NewGuid().ToString();
        ContextBag fistAttemptContextBag = configuration.GetContextBagForOutbox();

        var transportOperations = new TransportOperation[]
        {
            new(Guid.NewGuid().ToString(), new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>()),
            new(Guid.NewGuid().ToString(), new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>())
        };

        using (var transaction = await configuration.OutboxStorage.BeginTransaction(fistAttemptContextBag))
        {
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, fistAttemptContextBag);

            await transaction.Commit();
        }

        await ExpireMetadataRecord(incomingMessageId);

        var result = await configuration.OutboxStorage.Get(incomingMessageId, configuration.GetContextBagForOutbox());
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task Should_return_fresh_entry_when_metadata_expired_but_phantom_records_beyond_query_size_limit_present()
    {
        var incomingMessageId = Guid.NewGuid().ToString();
        ContextBag contextBag = configuration.GetContextBagForOutbox();

        var r = new Random();
        byte[] payload = new byte[100_000]; // 400 KB is the item size limit
        r.NextBytes(payload);

        // Query size limit is 1MB
        var transportOperations = new TransportOperation[20];
        for (int i = 0; i < transportOperations.Length; i++)
        {
            transportOperations[i] = new TransportOperation(Guid.NewGuid().ToString(), new DispatchProperties(),
                payload, new Dictionary<string, string>());
        }

        using (var transaction = await configuration.OutboxStorage.BeginTransaction(contextBag))
        {
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, contextBag);

            await transaction.Commit();
        }

        await ExpireMetadataRecord(incomingMessageId);

        var result = await configuration.OutboxStorage.Get(incomingMessageId, configuration.GetContextBagForOutbox());
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task Should_return_fresh_entry_when_metadata_expired_but_phantom_record_overlap()
    {
        var incomingMessageId = Guid.NewGuid().ToString();
        ContextBag fistAttemptContextBag = configuration.GetContextBagForOutbox();

        var transportOperations = new TransportOperation[]
        {
            new(Guid.NewGuid().ToString(), new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>()),
            new(Guid.NewGuid().ToString(), new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>()),
            new(Guid.NewGuid().ToString(), new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>()),
            new(Guid.NewGuid().ToString(), new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>())
        };

        using (var transaction = await configuration.OutboxStorage.BeginTransaction(fistAttemptContextBag))
        {
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, fistAttemptContextBag);

            await transaction.Commit();
        }

        await ExpireMetadataRecord(incomingMessageId);

        var transportOperationMessageId1 = Guid.NewGuid().ToString();
        var transportOperationMessageId2 = Guid.NewGuid().ToString();
        transportOperations = new TransportOperation[]
        {
            new(transportOperationMessageId1, new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>()),
            new(transportOperationMessageId2, new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>()),
        };

        using (var transaction = await configuration.OutboxStorage.BeginTransaction(fistAttemptContextBag))
        {
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, fistAttemptContextBag);

            await transaction.Commit();
        }

        var result = await configuration.OutboxStorage.Get(incomingMessageId, configuration.GetContextBagForOutbox());
        Assert.AreEqual(transportOperations.Length, result.TransportOperations.Length);
        Assert.That(result.TransportOperations[0].MessageId, Is.EqualTo(transportOperationMessageId1));
        Assert.That(result.TransportOperations[1].MessageId, Is.EqualTo(transportOperationMessageId2));
    }

    [Test]
    public async Task Should_return_fresh_entry_when_metadata_expired_but_phantom_record_overlap_beyond_query_size()
    {
        var incomingMessageId = Guid.NewGuid().ToString();
        ContextBag fistAttemptContextBag = configuration.GetContextBagForOutbox();

        var r = new Random();
        byte[] payload = new byte[100_000]; // 400 KB is the item size limit
        r.NextBytes(payload);

        // Query size limit is 1MB
        var transportOperations = new TransportOperation[20];
        for (int i = 0; i < transportOperations.Length; i++)
        {
            transportOperations[i] = new TransportOperation(Guid.NewGuid().ToString(), new DispatchProperties(),
                payload, new Dictionary<string, string>());
        }

        using (var transaction = await configuration.OutboxStorage.BeginTransaction(fistAttemptContextBag))
        {
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, fistAttemptContextBag);

            await transaction.Commit();
        }

        await ExpireMetadataRecord(incomingMessageId);

        var transportOperationMessageId1 = Guid.NewGuid().ToString();
        var transportOperationMessageId2 = Guid.NewGuid().ToString();
        transportOperations = new TransportOperation[]
        {
            new(transportOperationMessageId1, new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>()),
            new(transportOperationMessageId2, new DispatchProperties(), ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>()),
        };

        using (var transaction = await configuration.OutboxStorage.BeginTransaction(fistAttemptContextBag))
        {
            var outboxMessage = new OutboxMessage(incomingMessageId, transportOperations);
            await configuration.OutboxStorage.Store(outboxMessage, transaction, fistAttemptContextBag);

            await transaction.Commit();
        }

        var result = await configuration.OutboxStorage.Get(incomingMessageId, configuration.GetContextBagForOutbox());
        Assert.AreEqual(transportOperations.Length, result.TransportOperations.Length);
        Assert.That(result.TransportOperations[0].MessageId, Is.EqualTo(transportOperationMessageId1));
        Assert.That(result.TransportOperations[1].MessageId, Is.EqualTo(transportOperationMessageId2));
    }

    static async Task ExpireMetadataRecord(string incomingMessageId)
    {
        var outboxTable = SetupFixture.OutboxTable;
        await SetupFixture.DynamoDBClient.DeleteItemAsync(new DeleteItemRequest
        {
            Key = new Dictionary<string, AttributeValue>(2)
            {
                { outboxTable.PartitionKeyName, new AttributeValue($"OUTBOX#PersistenceTest#{incomingMessageId}") },
                { outboxTable.SortKeyName, new AttributeValue($"OUTBOX#METADATA#{incomingMessageId}") }
            },
            TableName = outboxTable.TableName
        });
    }
}