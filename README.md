# NServiceBus.Persistence.DynamoDB

NServiceBus persistence for [DynamoDB](https://aws.amazon.com/dynamodb/).

Documentation, including usage and samples, is available on the [Particular docs site](https://docs.particular.net/persistence/dynamodb/).

## How to Test Locally

Tests require an AWS account with permissions to create DynamoDB tables. Create an [access key](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys) in the AWS portal and configure the `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` environment variables.

You can also use the local DynamoDB docker container via `docker run -p 8000:8000 --name dynamodb amazon/dynamodb-local`. When using a local DynamoDB instance, the `ServiceUrl` needs to be set to `http://localhost:8000`.