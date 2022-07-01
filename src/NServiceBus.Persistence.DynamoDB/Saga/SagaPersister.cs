namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Extensibility;
    using Sagas;

    class SagaPersister : ISagaPersister
    {
#pragma warning disable IDE0060
        public SagaPersister(SagaPersistenceConfiguration options)
#pragma warning restore IDE0060
        {
        }

        public Task Save(IContainSagaData sagaData, SagaCorrelationProperty correlationProperty, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public Task Update(IContainSagaData sagaData, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public Task<TSagaData> Get<TSagaData>(Guid sagaId, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default) where TSagaData : class, IContainSagaData
        {
            return Task.FromResult<TSagaData>(default);
        }

        public Task<TSagaData> Get<TSagaData>(string propertyName, object propertyValue, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
            where TSagaData : class, IContainSagaData
        {
            // Saga ID needs to be calculated the same way as in SagaIdGenerator does
            // var sagaId = CosmosSagaIdGenerator.Generate(typeof(TSagaData), propertyName, propertyValue);
            return Task.FromResult<TSagaData>(default);
        }

        public Task Complete(IContainSagaData sagaData, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }
}