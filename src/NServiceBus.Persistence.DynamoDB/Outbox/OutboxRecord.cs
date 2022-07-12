namespace NServiceBus.Persistence.DynamoDB
{
    using System;

    class OutboxRecord
    {
        public string Id { get; set; }

        public bool Dispatched { get; set; }

        public StorageTransportOperation[] TransportOperations { get; set; } = Array.Empty<StorageTransportOperation>();
    }
}