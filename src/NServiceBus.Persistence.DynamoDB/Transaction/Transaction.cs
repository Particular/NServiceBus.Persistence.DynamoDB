namespace NServiceBus.Persistence.DynamoDB
{
    using Features;

    class Transaction : Feature
    {
        protected override void Setup(FeatureConfigurationContext context)
        {
            var configuration = context.Settings.GetOrDefault<TransactionInformationConfiguration>() ?? new TransactionInformationConfiguration();

            context.Pipeline.Register(new TransactionInformationBeforeThePhysicalOutboxBehavior.RegisterStep(configuration.PartitionKeyExtractor));
        }
    }
}