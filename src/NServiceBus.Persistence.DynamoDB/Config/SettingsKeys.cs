namespace NServiceBus.Persistence.DynamoDB
{
    static class SettingsKeys
    {
        const string BaseName = "DynamoDB.";
        public const string OutboxTableName = nameof(BaseName) + nameof(OutboxTableName);
        public const string SagasTableName = nameof(BaseName) + nameof(SagasTableName);
        public const string OutboxTimeToLive = nameof(BaseName) + nameof(OutboxTimeToLive);
    }
}