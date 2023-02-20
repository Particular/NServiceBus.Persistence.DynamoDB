using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace NServiceBus.Persistence.DynamoDB
{
    class InstallerSettings
    {
        public bool Disabled { get; set; }
        public bool CreateOutboxTable { get; set; }
        public string OutboxTableName { get; set; }
        public string OutboxPartitionKeyName { get; set; } = "PK";
        public string OutboxSortKeyName { get; set; } = "SK";

        public bool CreateSagaTable { get; set; }
        public string SagaTableName { get; set; }

        //TODO should we allow different configurations for each table?
        //TODO should we define our own billingmode type to avoid dependencies/breaking changes?
        public BillingMode BillingMode { get; set; } = BillingMode.PAY_PER_REQUEST;
        public ProvisionedThroughput ProvisionedThroughput { get; set; } // required when using billingmode.provisioned
    }
}