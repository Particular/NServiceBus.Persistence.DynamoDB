namespace NServiceBus.Persistence.DynamoDB;

using Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

class SynchronizedStorage : Feature
{
    public SynchronizedStorage() =>
        // Depends on the core feature
        DependsOn<Features.SynchronizedStorage>();

    protected override void Setup(FeatureConfigurationContext context)
    {
        context.Services.TryAddSingleton(context.Settings.Get<IDynamoClientProvider>());

        context.Services.AddScoped<ICompletableSynchronizedStorageSession, DynamoSynchronizedStorageSession>();
        context.Services.AddScoped(sp => (sp.GetService<ISynchronizedStorageSession>() as IDynamoStorageSession)!);
    }
}