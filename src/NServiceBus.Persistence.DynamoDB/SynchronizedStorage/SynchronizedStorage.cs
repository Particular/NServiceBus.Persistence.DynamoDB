namespace NServiceBus.Persistence.DynamoDB
{
    using System.Linq;
    using Features;
    using Microsoft.Extensions.DependencyInjection;

    class SynchronizedStorage : Feature
    {
        public SynchronizedStorage() =>
            // Depends on the core feature
            DependsOn<Features.SynchronizedStorage>();

        protected override void Setup(FeatureConfigurationContext context)
        {
            if (!context.Services.Any(descriptor => descriptor.ServiceType == typeof(IDynamoDBClientProvider)))
            {
                context.Services.AddSingleton(context.Settings.Get<IDynamoDBClientProvider>());
            }

            context.Services.AddScoped<ICompletableSynchronizedStorageSession, DynamoDBSynchronizedStorageSession>();
            context.Services.AddScoped(sp => sp.GetRequiredService<ICompletableSynchronizedStorageSession>().DynamoDBPersistenceSession());
        }
    }
}