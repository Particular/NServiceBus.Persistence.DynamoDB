namespace NServiceBus.Persistence.DynamoDB;

static class OutboxAttributeNames
{
    public const string Dispatched = nameof(Dispatched);
    public const string DispatchedAt = nameof(DispatchedAt);
    public const string OperationsCount = nameof(OperationsCount);
    public const string MessageId = nameof(MessageId);
    public const string Properties = nameof(Properties);
    public const string Headers = nameof(Headers);
    public const string Body = nameof(Body);
    public const string SchemaVersion = nameof(SchemaVersion);
}