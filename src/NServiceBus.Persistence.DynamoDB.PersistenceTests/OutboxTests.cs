namespace NServiceBus.PersistenceTesting
{
    using System.Threading.Tasks;
    using System;
    using System.Collections.Generic;
    using Amazon.DynamoDBv2;
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
    }
}