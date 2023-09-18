namespace NServiceBus.AcceptanceTests;

using System.Runtime.CompilerServices;
using AcceptanceTesting;
using AcceptanceTesting.Support;

public class TestSuiteConstraints : ITestSuiteConstraints
{
    public bool SupportsDtc { get; } = false;
    public bool SupportsCrossQueueTransactions { get; } = true;
    public bool SupportsNativePubSub { get; } = true;
    public bool SupportsOutbox { get; } = true;
    public bool SupportsDelayedDelivery { get; } = true;
    public bool SupportsPurgeOnStartup { get; } = true;

    public IConfigureEndpointTestExecution CreateTransportConfiguration() => new ConfigureEndpointAcceptanceTestingTransport(true, true);

    public IConfigureEndpointTestExecution CreatePersistenceConfiguration() => new ConfigureEndpointDynamoDBPersistence();

    [ModuleInitializer]
    public static void Init() => ITestSuiteConstraints.Current = new TestSuiteConstraints();
}