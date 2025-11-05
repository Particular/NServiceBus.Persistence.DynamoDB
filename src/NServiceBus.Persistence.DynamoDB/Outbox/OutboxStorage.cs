namespace NServiceBus.Persistence.DynamoDB;

using System;
using Features;
using Microsoft.Extensions.DependencyInjection;
using Outbox;

class OutboxStorage : Feature
{
    OutboxStorage()
    {
        EnableByDefault<SynchronizedStorage>();
        DependsOn<Outbox>();
        DependsOn<SynchronizedStorage>();
    }

    protected override void Setup(FeatureConfigurationContext context)
    {
        OutboxPersistenceConfiguration outboxConfiguration = context.Settings.Get<OutboxPersistenceConfiguration>();

        ValidateOutboxSettings(outboxConfiguration);

        var endpointName = string.IsNullOrEmpty(outboxConfiguration.ProcessorEndpoint) ? context.Settings.EndpointName() : outboxConfiguration.ProcessorEndpoint;

        context.Services.AddSingleton<IOutboxStorage>(provider => new OutboxPersister(provider.GetRequiredService<IDynamoClientProvider>().Client, outboxConfiguration, endpointName));
    }

    void ValidateOutboxSettings(OutboxPersistenceConfiguration outboxConfiguration)
    {
        if (outboxConfiguration.Table.TimeToLiveAttributeName == null)
        {
            throw new InvalidOperationException(
                $"The outbox persistence table requires a time-to-live attribute to be defined. Use the '{nameof(OutboxPersistenceConfiguration)}.{nameof(OutboxPersistenceConfiguration.Table)}.{nameof(TableConfiguration.TimeToLiveAttributeName)}' setting to specify the name of the time-to-live attribute.");
        }
    }
}