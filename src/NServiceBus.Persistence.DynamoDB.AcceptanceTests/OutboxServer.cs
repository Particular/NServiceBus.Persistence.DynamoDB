namespace NServiceBus.AcceptanceTests.EndpointTemplates;

using System;
using System.Threading.Tasks;
using AcceptanceTesting.EndpointTemplates;
using AcceptanceTesting.Support;

public class OutboxServer : DefaultServer
{
    public override Task<EndpointConfiguration> GetConfiguration(RunDescriptor runDescriptor, EndpointCustomizationConfiguration endpointConfiguration,
        Func<EndpointConfiguration, Task> configurationBuilderCustomization) =>
        base.GetConfiguration(runDescriptor, endpointConfiguration, configuration =>
        {
            configuration.ConfigureTransport().TransportTransactionMode = TransportTransactionMode.ReceiveOnly;
            configuration.EnableOutbox();

            return configurationBuilderCustomization(configuration);
        });
}