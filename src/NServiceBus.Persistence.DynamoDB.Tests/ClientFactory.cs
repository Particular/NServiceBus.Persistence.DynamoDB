namespace NServiceBus.Persistence.DynamoDB.Tests;

using System;
using Amazon.DynamoDBv2;
using Amazon.Runtime;

public static class ClientFactory
{
    public static IAmazonDynamoDB CreateDynamoDBClient(Action<AmazonDynamoDBConfig> configure = default)
    {
        AWSCredentials credentials = new BasicAWSCredentials("localdb", "localdb");

        var config = new AmazonDynamoDBConfig();
        configure?.Invoke(config);


        config.ServiceURL = Environment.GetEnvironmentVariable("AWS_DYNAMODB_LOCAL_ADDRESS") ??
                            "http://localhost:8000";

        return new AmazonDynamoDBClient(credentials, config);
    }
}