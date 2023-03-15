namespace NServiceBus.PersistenceTesting
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using NServiceBus.Outbox;
    using NServiceBus.Sagas;
    using NUnit.Framework;
    using Persistence;
    using Persistence.DynamoDB;

    public partial class PersistenceTestsConfiguration : IDynamoDBClientProvider
    {
        static PersistenceTestsConfiguration()
        {
            SagaVariants = new[]
            {
                new TestFixtureData(new TestVariant(new PersistenceConfiguration(usePessimisticLocking: false))).SetArgDisplayNames("Optimistic"),
                new TestFixtureData(new TestVariant(new PersistenceConfiguration(usePessimisticLocking: true))).SetArgDisplayNames("Pessimistic"),
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

        public Task Configure(CancellationToken cancellationToken = default)
        {
            var configuration = (PersistenceConfiguration)Variant.Values[0];

            SagaStorage = new SagaPersister(
                new SagaPersistenceConfiguration
                {
                    Table = SetupFixture.SagaTable,
                    UsePessimisticLocking = SupportsPessimisticConcurrency = configuration.UsePessimisticLocking,
                    LeaseAcquisitionTimeout = Variant.SessionTimeout ?? TimeSpan.FromSeconds(10)
                },
                Client);

            OutboxStorage = new OutboxPersister(
                Client,
                new OutboxPersistenceConfiguration
                {
                    Table = SetupFixture.OutboxTable
                },
                "PersistenceTest");

            //TODO define TTL value which was 100 sec
            CreateStorageSession = () => new DynamoDBSynchronizedStorageSession(this);

            return Task.CompletedTask;
        }

        public Task Cleanup(CancellationToken cancellationToken = default) =>
            // Cleanup is done by the setup fixture
            Task.CompletedTask;
    }
}