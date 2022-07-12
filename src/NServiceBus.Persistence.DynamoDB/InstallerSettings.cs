namespace NServiceBus.Persistence.DynamoDB
{
    class InstallerSettings
    {
        public bool Disabled { get; set; }
        public string OutboxTableName { get; set; }
        public string SagaTableName { get; set; }
    }
}