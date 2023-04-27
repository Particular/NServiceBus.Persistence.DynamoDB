namespace NServiceBus.Persistence.DynamoDB;

static class OutboxAttributeNames
{
    public const string Dispatched = "DISPATCHED";
    public const string DispatchedAt = "DISPATCHED_AT";
    public const string OperationsCount = "OPERATIONS_COUNT";
    public const string MessageId = "MESSAGE_ID";
    public const string Properties = "PROPERTIES";
    public const string Headers = "HEADERS";
    public const string Body = "BODY";
    public const string SchemaVersion = "SCHEMA_VERSION";
}