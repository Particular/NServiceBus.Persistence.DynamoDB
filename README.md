# NServiceBus.Persistence.DynamoDB

NServiceBus.Persistence.DynamoDB is the official NServiceBus persistence for [DynamoDB](https://aws.amazon.com/dynamodb/).

It is part of the [Particular Service Platform](https://particular.net/service-platform), which includes [NServiceBus](https://particular.net/nservicebus) and tools to build, monitor, and debug distributed systems.

See the [AWS DynamoDB persistence documentation](https://docs.particular.net/persistence/dynamodb/) to learn more.

## Running tests locally

Tests require an AWS account with permissions to create DynamoDB tables. Create an [access key](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys) in the AWS portal and configure the `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` environment variables.

Alternatively, use the local DynamoDB docker container via `docker run -p 8000:8000 --name dynamodb amazon/dynamodb-local` (for more information refer to the [local DynamoDB installation guidance](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html)). The tests will automatically use the local DynamoDB instance when no `AWS_ACCESS_KEY_ID` environment variable has been configured. By default, the tests assume the local DynamoDB instance is available under `http://localhost:8000`. To use a different host or port, set the `AWS_DYNAMODB_LOCAL_ADDRESS` environment variable to the custom URL.
