namespace NServiceBus.Persistence.DynamoDB
{
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;

    class InstallerSettings
    {
        //TODO remove?
        public bool Disabled { get; set; }
        public bool CreateOutboxTable { get; set; } = true;

        public bool CreateSagaTable { get; set; } = true;
        public string SagaTableName { get; set; }

        //TODO should we allow different configurations for each table?
        //TODO should we define our own billingmode type to avoid dependencies/breaking changes?
        public BillingMode BillingMode { get; set; } = BillingMode.PAY_PER_REQUEST;
        public ProvisionedThroughput ProvisionedThroughput { get; set; } // required when using billingmode.provisioned
    }
}