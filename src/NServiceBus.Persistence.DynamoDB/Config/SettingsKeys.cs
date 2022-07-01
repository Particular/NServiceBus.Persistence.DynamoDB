namespace NServiceBus.Persistence.DynamoDB
{
    static class SettingsKeys
    {
        const string BaseName = "DynamoDB.";
        public const string TableName = nameof(BaseName) + nameof(TableName);
        public const string OutboxTimeToLiveInSeconds = nameof(BaseName) + nameof(OutboxTimeToLiveInSeconds);
    }
}