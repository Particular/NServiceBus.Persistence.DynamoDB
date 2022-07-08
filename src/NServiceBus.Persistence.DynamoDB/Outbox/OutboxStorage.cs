namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using Features;
    using Microsoft.Extensions.DependencyInjection;
    using Outbox;

    class OutboxStorage : Feature
    {
        OutboxStorage()
        {
            Defaults(s =>
            {
                s.SetDefault(SettingsKeys.OutboxTimeToLive, TimeSpan.FromDays(7));
                // TODO: Let's make sure the endpoint name adheres to the naming rules
                s.SetDefault(SettingsKeys.OutboxTableName, s.EndpointName());
                s.EnableFeatureByDefault<SynchronizedStorage>();
            });

            DependsOn<Outbox>();
            DependsOn<SynchronizedStorage>();
        }

        protected override void Setup(FeatureConfigurationContext context)
        {
            NonNativePubSubCheck.ThrowIfMessageDrivenPubSubInUse(context);

            var expirationPeriod = context.Settings.Get<TimeSpan>(SettingsKeys.OutboxTimeToLive);
            string tableName = context.Settings.Get<string>(SettingsKeys.OutboxTableName);

            context.Services.AddSingleton<IOutboxStorage>(provider => new OutboxPersister(provider.GetRequiredService<IProvideDynamoDBClient>().Client, tableName, expirationPeriod));
        }
    }
}