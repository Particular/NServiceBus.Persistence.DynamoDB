﻿namespace NServiceBus.Persistence.DynamoDB;

using Features;
using Microsoft.Extensions.DependencyInjection;

class InstallerFeature : Feature
{
    public InstallerFeature()
    {
        DependsOn<SynchronizedStorage>();
    }

    protected override void Setup(FeatureConfigurationContext context) =>
        context.Services.AddSingleton(sp => new Installer(sp.GetRequiredService<IDynamoClientProvider>().Client));
}