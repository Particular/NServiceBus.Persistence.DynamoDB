namespace NServiceBus
{
    using System;
    using Persistence;
    using Persistence.DynamoDB;

    /// <summary>
    /// DynamoDB persistence specific extension methods for the <see cref="ISynchronizedStorageSession"/>.
    /// </summary>
    public static class SynchronizedStorageSessionExtensions
    {
        /// <summary>
        /// Retrieves the shared <see cref="IDynamoDBStorageSession"/> from the <see cref="ISynchronizedStorageSession"/>.
        /// </summary>
        public static IDynamoDBStorageSession DynamoDBPersistenceSession(this ISynchronizedStorageSession session)
        {
            Guard.AgainstNull(nameof(session), session);

            if (session is not IDynamoDBStorageSession dynamoSession)
            {
                throw new Exception($"Cannot access the synchronized storage session. Ensure that 'EndpointConfiguration.UsePersistence<{nameof(DynamoDBPersistence)}>()' has been called.");
            }
            return dynamoSession;
        }
    }
}