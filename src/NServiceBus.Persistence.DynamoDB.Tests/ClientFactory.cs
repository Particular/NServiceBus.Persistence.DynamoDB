namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System;
    using Amazon.DynamoDBv2;
    using Amazon.Runtime;

    public static class ClientFactory
    {
        public static IAmazonDynamoDB CreateDynamoDBClient(Action<AmazonDynamoDBConfig> configure = default)
        {
            var credentials = new EnvironmentVariablesAWSCredentials();
            var config = Create(configure);
            return new AmazonDynamoDBClient(credentials, config);
        }

        // Can be removed once https://github.com/aws/aws-sdk-net/issues/1929 is addressed by the team
        // setting the cache size to 1 will significantly improve the throughput on non-windows OSS while
        // windows had already 1 as the default.
        // There might be other occurrences of setting this setting explicitly in the code base. Make sure to remove them
        // consistently once the issue is addressed.
        // The method is deliberately generic because this config should be set on any SDK client.
        static TConfig Create<TConfig>(Action<TConfig> configure = default)
            where TConfig : ClientConfig, new()
        {
#if NET
            var config = new TConfig { HttpClientCacheSize = 1 };
#else
            var config = new TConfig();
#endif
            configure?.Invoke(config);
            return config;
        }
    }
}