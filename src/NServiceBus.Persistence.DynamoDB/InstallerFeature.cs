namespace NServiceBus.Persistence.DynamoDB
{
    using Features;
    using Microsoft.Extensions.DependencyInjection;

    class InstallerFeature : Feature
    {
        public InstallerFeature()
        {
            DependsOn<SynchronizedStorage>();
        }

        protected override void Setup(FeatureConfigurationContext context)
        {
            var installer = new Installer(context.Settings.Get<IDynamoDBClientProvider>().Client);
            context.Services.AddSingleton(installer);
        }
    }
}