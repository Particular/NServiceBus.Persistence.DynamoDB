namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Threading;
    using Amazon.DynamoDBv2;
    using Amazon.Runtime;

    // This type will be tracked by the container and therefore disposed by the container
    sealed class DefaultDynamoDbClientProviderProvider : IDynamoDBClientProvider, IDisposable
    {
        public IAmazonDynamoDB Client => client.Value;

        public void Dispose()
        {
            if (client.IsValueCreated)
            {
                client.Value.Dispose();
            }
        }

        static IAmazonDynamoDB CreateDynamoDBClient()
        {
            var config = Create<AmazonDynamoDBConfig>();
            return new AmazonDynamoDBClient(config);
        }

        // Can be removed once https://github.com/aws/aws-sdk-net/issues/1929 is addressed by the team
        // setting the cache size to 1 will significantly improve the throughput on non-windows OSS while
        // windows had already 1 as the default.
        // There might be other occurrences of setting this setting explicitly in the code base. Make sure to remove them
        // consistently once the issue is addressed.
        // The method is deliberately generic because this config should be set on any SDK client.
        static TConfig Create<TConfig>()
            where TConfig : ClientConfig, new()
        {
#if NET
            var config = new TConfig { HttpClientCacheSize = 1 };
#else
            var config = new TConfig();
#endif
            return config;
        }

        readonly Lazy<IAmazonDynamoDB> client = new(CreateDynamoDBClient, LazyThreadSafetyMode.ExecutionAndPublication);
    }
}