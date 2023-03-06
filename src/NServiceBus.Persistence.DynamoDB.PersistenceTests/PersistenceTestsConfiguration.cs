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

            // TODO consider splitting up table config from "persister config" to decouple from the static setupfixture
            SetupFixture.SagaConfiguration.UsePessimisticLocking =
                SupportsPessimisticConcurrency = configuration.UsePessimisticLocking;
            SetupFixture.SagaConfiguration.LeaseAcquistionTimeout = Variant.SessionTimeout ?? TimeSpan.FromSeconds(10);

            SagaStorage = new SagaPersister(SetupFixture.SagaConfiguration, Client);
            OutboxStorage = new OutboxPersister(
                Client,
                SetupFixture.OutboxConfiguration,
                "PersistenceTest");

            CreateStorageSession = () => new DynamoDBSynchronizedStorageSession(this);

            return Task.CompletedTask;
        }

        public Task Cleanup(CancellationToken cancellationToken = default) =>
            // Cleanup is done by the setup fixture
            Task.CompletedTask;
    }
}