namespace NServiceBus.PersistenceTesting;

using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using NServiceBus.Outbox;
using NServiceBus.Sagas;
using NUnit.Framework;
using Persistence;
using Persistence.DynamoDB;

public partial class PersistenceTestsConfiguration : IDynamoClientProvider
{
    static PersistenceTestsConfiguration()
    {
        SagaVariants =
        [
            new TestFixtureData(new TestVariant(new PersistenceConfiguration())).SetArgDisplayNames("Optimistic"),
            new TestFixtureData(new TestVariant(new PersistenceConfiguration(UseEventualConsistentReads: true))).SetArgDisplayNames("Optimistic Eventual Consistent"),
            new TestFixtureData(new TestVariant(new PersistenceConfiguration(UsePessimisticLocking: true))).SetArgDisplayNames("Pessimistic")
        ];

        OutboxVariants =
        [
            new TestFixtureData(new TestVariant(new PersistenceConfiguration())).SetArgDisplayNames("Optimistic"),
        ];
    }

    public record PersistenceConfiguration(bool? UsePessimisticLocking = null, bool? UseEventualConsistentReads = null);

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

        var sagaPersistenceConfiguration = new SagaPersistenceConfiguration
        {
            Table = SetupFixture.SagaTable,
            LeaseAcquisitionTimeout = Variant.SessionTimeout ?? TimeSpan.FromSeconds(10)
        };

        if (configuration.UsePessimisticLocking.HasValue)
        {
            sagaPersistenceConfiguration.UsePessimisticLocking = SupportsPessimisticConcurrency = configuration.UsePessimisticLocking.Value;
        }

        if (configuration.UseEventualConsistentReads.HasValue)
        {
            sagaPersistenceConfiguration.UseEventualConsistentReads = configuration.UseEventualConsistentReads.Value;
        }

        SagaStorage = new SagaPersister(
            Client,
            sagaPersistenceConfiguration,
            "PersistenceTest");

        OutboxStorage = new OutboxPersister(
            Client,
            new OutboxPersistenceConfiguration
            {
                Table = SetupFixture.OutboxTable,
                TimeToKeepDeduplicationData = TimeSpan.FromSeconds(100)
            },
            "PersistenceTest");

        CreateStorageSession = () => new DynamoSynchronizedStorageSession(this);

        return Task.CompletedTask;
    }

    public Task Cleanup(CancellationToken cancellationToken = default) =>
        // Cleanup is done by the setup fixture
        Task.CompletedTask;
}