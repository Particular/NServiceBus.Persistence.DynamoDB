namespace NServiceBus.Persistence.DynamoDB
{
    using Features;
    using Microsoft.Extensions.DependencyInjection;
    using Outbox;

    class OutboxStorage : Feature
    {
        OutboxStorage()
        {
            Defaults(s =>
            {
                s.EnableFeatureByDefault<SynchronizedStorage>();
            });

            DependsOn<Outbox>();
            DependsOn<SynchronizedStorage>();
        }

        protected override void Setup(FeatureConfigurationContext context)
        {
            OutboxPersistenceConfiguration outboxConfiguration = context.Settings.Get<OutboxPersistenceConfiguration>();

            context.Services.AddSingleton<IOutboxStorage>(provider => new OutboxPersister(provider.GetRequiredService<IProvideDynamoDBClient>().Client, outboxConfiguration));
        }
    }
}