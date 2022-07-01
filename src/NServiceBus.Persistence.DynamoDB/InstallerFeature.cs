namespace NServiceBus.Persistence.DynamoDB
{
    using Features;
    using Microsoft.Extensions.DependencyInjection;

    class InstallerFeature : Feature
    {
        public InstallerFeature()
        {
            Defaults(s => s.SetDefault(new InstallerSettings()));
            DependsOn<SynchronizedStorage>();
        }

        protected override void Setup(FeatureConfigurationContext context)
        {
            var settings = context.Settings.Get<InstallerSettings>();
            context.Services.AddSingleton(settings);
            if (settings.Disabled)
            {
                return;
            }

            var tableName = context.Settings.Get<string>(SettingsKeys.TableName);
            settings.TableName = tableName;
        }
    }
}