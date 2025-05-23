namespace NServiceBus.TransactionalSession.AcceptanceTests;

using System;
using System.Threading.Tasks;
using AcceptanceTesting;
using AcceptanceTesting.Customization;
using AcceptanceTesting.Support;
using Configuration.AdvancedExtensibility;

public class DefaultServer : IEndpointSetupTemplate
{
    public virtual async Task<EndpointConfiguration> GetConfiguration(RunDescriptor runDescriptor,
        EndpointCustomizationConfiguration endpointCustomization,
        Func<EndpointConfiguration, Task> configurationBuilderCustomization)
    {
        var endpointConfiguration = new EndpointConfiguration(endpointCustomization.EndpointName);

        endpointConfiguration.UseSerialization<SystemJsonSerializer>();
        endpointConfiguration.UseTransport<LearningTransport>();

        endpointConfiguration.Recoverability()
            .Delayed(delayed => delayed.NumberOfRetries(0))
            // due to read-committed isolation level we allow retries for partial results on the outbox get
            .Immediate(immediate => immediate.NumberOfRetries(1));

        // scan types at the end so that all types used by the configuration have been loaded into the AppDomain
        endpointConfiguration.TypesToIncludeInScan(endpointCustomization.GetTypesScopedByTestClass());

        var persistence = endpointConfiguration.UsePersistence<DynamoPersistence>();
        persistence.DynamoClient(SetupFixture.DynamoDBClient);
        persistence.UseSharedTable(SetupFixture.TableConfiguration);
        endpointConfiguration.GetSettings().Set(persistence);

        if (runDescriptor.ScenarioContext is TransactionalSessionTestContext testContext)
        {
            endpointConfiguration.RegisterStartupTask(sp => new CaptureServiceProviderStartupTask(sp, testContext, endpointCustomization.EndpointName));
        }
        await configurationBuilderCustomization(endpointConfiguration).ConfigureAwait(false);

        return endpointConfiguration;
    }
}