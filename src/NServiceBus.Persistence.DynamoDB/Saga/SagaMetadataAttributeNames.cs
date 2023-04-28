namespace NServiceBus.Persistence.DynamoDB;

static class SagaMetadataAttributeNames
{
    public const string Metadata = "NServiceBus_Metadata";
    public const string Version = "Version";
    public const string SagaDataType = "Type";
    public const string SchemaVersion = nameof(SchemaVersion);
    public const string LeaseTimeout = "NServiceBus_LeaseTimeout";
}