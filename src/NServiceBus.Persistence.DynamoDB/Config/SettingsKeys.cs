namespace NServiceBus.Persistence.DynamoDB
{
    static class SettingsKeys
    {
        const string BaseName = "DynamoDB.";
        public const string TableName = nameof(BaseName) + nameof(TableName);
        public const string OutboxTimeToLive = nameof(BaseName) + nameof(OutboxTimeToLive);
    }
}