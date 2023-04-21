namespace NServiceBus.Persistence.DynamoDB
{
    static class SagaMetadataAttributeNames
    {
        public const string SagaMetadataAttributeName = "NSERVICEBUS_METADATA";
        public const string SagaDataVersionAttributeName = "VERSION";
        public const string SagaLeaseAttributeName = "LEASE_TIMEOUT";
    }
}