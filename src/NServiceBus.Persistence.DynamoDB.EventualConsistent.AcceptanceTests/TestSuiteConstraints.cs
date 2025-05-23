﻿namespace NServiceBus.AcceptanceTests;

using AcceptanceTesting.Support;

public partial class TestSuiteConstraints
{
    public bool SupportsDtc { get; } = false;
    public bool SupportsCrossQueueTransactions { get; } = true;
    public bool SupportsNativePubSub { get; } = true;
    public bool SupportsOutbox { get; } = true;
    public bool SupportsDelayedDelivery { get; } = true;
    public bool SupportsPurgeOnStartup { get; } = true;

    public IConfigureEndpointTestExecution CreateTransportConfiguration() => new ConfigureEndpointAcceptanceTestingTransport(true, true, TransportTransactionMode.ReceiveOnly);

    public IConfigureEndpointTestExecution CreatePersistenceConfiguration() => new ConfigureEndpointDynamoDBPersistence();
}