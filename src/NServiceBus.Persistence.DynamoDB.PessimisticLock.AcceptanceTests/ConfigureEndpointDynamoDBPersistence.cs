using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.AcceptanceTesting.Support;
using NServiceBus.AcceptanceTests;
using NServiceBus.Configuration.AdvancedExtensibility;
using NServiceBus.Persistence.DynamoDB;

public class ConfigureEndpointDynamoDBPersistence : IConfigureEndpointTestExecution
{
    public Task Configure(string endpointName, EndpointConfiguration configuration, RunSettings settings, PublisherMetadata publisherMetadata)
    {
        if (configuration.GetSettings().Get<bool>("Endpoint.SendOnly"))
        {
            return Task.CompletedTask;
        }

        // disable installers which are enabled by default in the standard endpoint templates
        configuration.GetSettings().Set("Installers.Enable", false);

        var persistence = configuration.UsePersistence<DynamoPersistence>();
        persistence.DynamoClient(SetupFixture.DynamoDBClient);
        persistence.UseSharedTable(SetupFixture.TableConfiguration);

        persistence.Sagas().UsePessimisticLocking = true;

        return Task.CompletedTask;
    }

    public Task Cleanup() => Task.CompletedTask;
}