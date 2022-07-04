namespace NServiceBus.PersistenceTesting
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Extensibility;
    using NServiceBus.Outbox;
    using NServiceBus.Sagas;
    using NUnit.Framework;
    using Persistence;
    using Persistence.DynamoDB;

    public partial class PersistenceTestsConfiguration : IProvideDynamoDBClient
    {
        static PersistenceTestsConfiguration()
        {
            SagaVariants = new[]
            {
                new TestFixtureData(new TestVariant(new PersistenceConfiguration(usePessimisticLocking: false))).SetArgDisplayNames("Optimistic"),
            };

            OutboxVariants = new[]
            {
                new TestFixtureData(new TestVariant(new PersistenceConfiguration(usePessimisticLocking: false))).SetArgDisplayNames("Optimistic"),
            };
        }
        public class PersistenceConfiguration
        {
            public readonly bool UsePessimisticLocking;

            public PersistenceConfiguration(bool usePessimisticLocking)
            {
                UsePessimisticLocking = usePessimisticLocking;
            }
        }


        public bool SupportsDtc => false;

        public bool SupportsOutbox => true;

        public bool SupportsFinders => false;

        public bool SupportsPessimisticConcurrency { get; private set; }

        public ISagaIdGenerator SagaIdGenerator { get; } = new SagaIdGenerator();

        public ISagaPersister SagaStorage { get; private set; }

        public IOutboxStorage OutboxStorage { get; private set; }

        public IAmazonDynamoDB Client { get; } = SetupFixture.DynamoDBClient;

        public Func<ICompletableSynchronizedStorageSession> CreateStorageSession { get; private set; }

        public int OutboxTimeToLiveInSeconds { get; set; } = 100;

        public Task Configure(CancellationToken cancellationToken = default)
        {
            // with this we have a partition key per run which makes things naturally isolated
            partitionKey = Guid.NewGuid().ToString();
            SagaStorage = null;
            OutboxStorage = new OutboxPersister(Client, SetupFixture.TableName, TimeSpan.FromMinutes(5));

            GetContextBagForSagaStorage = () =>
            {
                var contextBag = new ContextBag();
                contextBag.Set(new PartitionKey(partitionKey));
                return contextBag;
            };

            GetContextBagForOutbox = () =>
            {
                var contextBag = new ContextBag();
                contextBag.Set(new PartitionKey(partitionKey));
                return contextBag;
            };

            CreateStorageSession = () => new DynamoDBSynchronizedStorageSession(new DynamoDBClientProvidedByConfiguration { Client = SetupFixture.DynamoDBClient });

            return Task.CompletedTask;
        }

        public Task Cleanup(CancellationToken cancellationToken = default)
        {
            // Cleanup is done by the setup fixture
            return Task.CompletedTask;
        }

        string partitionKey;
    }
}