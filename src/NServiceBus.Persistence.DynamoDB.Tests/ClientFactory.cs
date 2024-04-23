namespace NServiceBus.Persistence.DynamoDB.Tests;

using System;
using Amazon.DynamoDBv2;
using Amazon.Runtime;

public static class ClientFactory
{
    public static IAmazonDynamoDB CreateDynamoDBClient(Action<AmazonDynamoDBConfig> configure = default)
    {
        var noAccessKey = string.IsNullOrEmpty(Environment.GetEnvironmentVariable(
            EnvironmentVariablesAWSCredentials.ENVIRONMENT_VARIABLE_ACCESSKEY));

        AWSCredentials credentials = noAccessKey
            ? new BasicAWSCredentials("localdb", "localdb")
            : new EnvironmentVariablesAWSCredentials();

        var config = new AmazonDynamoDBConfig();
        configure?.Invoke(config);

        if (noAccessKey)
        {
            config.ServiceURL = Environment.GetEnvironmentVariable("AWS_DYNAMODB_LOCAL_ADDRESS") ??
                                    "http://localhost:8000";
        }

        return new AmazonDynamoDBClient(credentials, config);
    }
}
