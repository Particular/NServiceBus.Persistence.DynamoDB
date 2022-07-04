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
                s.EnableFeatureByDefault<SynchronizedStorage>();
            });

            DependsOn<Outbox>();
            DependsOn<SynchronizedStorage>();
        }

        protected override void Setup(FeatureConfigurationContext context)
        {
            NonNativePubSubCheck.ThrowIfMessageDrivenPubSubInUse(context);

            var expirationPeriod = context.Settings.Get<TimeSpan>(SettingsKeys.OutboxTimeToLive);

            context.Services.AddSingleton<IOutboxStorage>(builder => new OutboxPersister(null, "MyTable", expirationPeriod));
        }
    }
}