namespace NServiceBus.PersistenceTesting
{
    using System.Collections.Generic;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;
    using NUnit.Framework;

    [TestFixture]
    public class DynamoSpecTests
    {
        [Test]
        public void Condition_failures_should_contain_todo_error_code()
        {
            var client = SetupFixture.DynamoDBClient;
            var sagaPersistenceConfiguration = SetupFixture.SagaConfiguration;

            var exception = Assert.CatchAsync<AmazonDynamoDBException>(() => client.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = sagaPersistenceConfiguration.TableName,
                Key = new Dictionary<string, AttributeValue>
                {
                    { sagaPersistenceConfiguration.PartitionKeyName, new AttributeValue("123") },
                    { sagaPersistenceConfiguration.SortKeyName, new AttributeValue("456") }
                },
                ConditionExpression = "attribute_exists(NonExistentAttribute)",
                UpdateExpression = "SET NonExistentAttribute = :update_value",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":update_value", new AttributeValue { N = "42" }}
                }
            }));

            Assert.AreEqual(exception.ErrorCode, "ConditionalCheckFailedException");
        }
    }
}