namespace NServiceBus.TransactionalSession.AcceptanceTests;

using System;
using System.Threading.Tasks;
using AcceptanceTesting;
using AcceptanceTesting.Support;
using AcceptanceTesting.Customization;

public class TransactionSessionDefaultServer : IEndpointSetupTemplate
{
    public virtual async Task<EndpointConfiguration> GetConfiguration(RunDescriptor runDescriptor,
        EndpointCustomizationConfiguration endpointConfiguration,
        Func<EndpointConfiguration, Task> configurationBuilderCustomization)
    {
        var builder = new EndpointConfiguration(endpointConfiguration.EndpointName);

        builder.UseTransport<LearningTransport>();

        builder.Recoverability()
            .Delayed(delayed => delayed.NumberOfRetries(0))
            .Immediate(immediate => immediate.NumberOfRetries(0))
            .CustomPolicy(RecoverabilityPolicy.Invoke);
        builder.SendFailedMessagesTo("error");

        // scan types at the end so that all types used by the configuration have been loaded into the AppDomain
        builder.TypesToIncludeInScan(endpointConfiguration.GetTypesScopedByTestClass());

        var persistence = builder.UsePersistence<DynamoPersistence>();
        persistence.DynamoClient(SetupFixture.DynamoDBClient);
        persistence.UseSharedTable(SetupFixture.TableConfiguration);
        persistence.EnableTransactionalSession();

        builder.RegisterStartupTask(sp => new CaptureServiceProviderStartupTask(sp, runDescriptor.ScenarioContext));

        await configurationBuilderCustomization(builder).ConfigureAwait(false);

        return builder;
    }
}