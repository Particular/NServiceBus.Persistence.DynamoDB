namespace NServiceBus.Persistence.DynamoDB
{
    class InstallerSettings
    {
        //TODO remove?
        public bool Disabled { get; set; }
        public bool CreateOutboxTable { get; set; } = true;

        public bool CreateSagaTable { get; set; } = true;
    }
}