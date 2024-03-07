namespace NServiceBus.TransactionalSession.AcceptanceTests;

using System;
using System.Threading.Tasks;
using AcceptanceTesting;
using AcceptanceTesting.Support;
using AcceptanceTesting.Customization;

public class TransactionSessionDefaultServer : IEndpointSetupTemplate
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
        persistence.EnableTransactionalSession();

        endpointConfiguration.RegisterStartupTask(sp => new CaptureServiceProviderStartupTask(sp, runDescriptor.ScenarioContext));

        await configurationBuilderCustomization(endpointConfiguration).ConfigureAwait(false);

        return endpointConfiguration;
    }
}