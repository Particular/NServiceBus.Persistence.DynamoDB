namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2.Model;
    using Logging;
    using NUnit.Framework;
    using Particular.Approvals;

    [TestFixture]
    public class WriteRequestBatchExecutionTests
    {
        [SetUp]
        public void SetUp()
        {
            configuration = new OutboxPersistenceConfiguration
            {
                TableName = "faketable",
                PartitionKeyName = "PK",
                SortKeyName = "SK",
            };
            logger = new Logger();
            client = new MockDynamoDBClient();
        }

        [Test]
        public async Task ExecutesBatches()
        {
            var batches = new List<List<WriteRequest>>
            {
                new() { new WriteRequest(), new WriteRequest() },
                new() { new WriteRequest(), new WriteRequest(), new WriteRequest() }
            };

            await client.BatchWriteItemWithRetries(batches, configuration, logger);

            Assert.That(client.BatchWriteRequestsSent, Has.Count.EqualTo(2));
            Assert.That(client.BatchWriteRequestsSent, Has.One.Matches<BatchWriteItemRequest>(x => x.RequestItems[configuration.TableName].Count == 2));
            Assert.That(client.BatchWriteRequestsSent, Has.One.Matches<BatchWriteItemRequest>(x => x.RequestItems[configuration.TableName].Count == 3));
        }

        [Test]
        public async Task LogsDetailsOnDebugLogging()
        {
            logger.IsDebugEnabled = true;

            // reusing the same attribute values for testing
            var attributeValues = new Dictionary<string, AttributeValue>
            {
                [configuration.PartitionKeyName] = new("PK"),
                [configuration.SortKeyName] = new("SK")
            };

            var batches = new List<List<WriteRequest>>
            {
                new() { new WriteRequest(new DeleteRequest(attributeValues)), new WriteRequest(new PutRequest(attributeValues)) },
                new() { new WriteRequest(new PutRequest(attributeValues)), new WriteRequest(new DeleteRequest(attributeValues)), new WriteRequest(new PutRequest(attributeValues)) }
            };

            await client.BatchWriteItemWithRetries(batches, configuration, logger);

            Assert.That(logger.InfoMessages, Is.Empty);
            Approver.Verify(logger.DebugMessages);
        }

        [Test]
        public async Task DoesNotLogDetailsByDefault()
        {
            var batches = new List<List<WriteRequest>>
            {
                new() { new WriteRequest(), new WriteRequest() },
                new() { new WriteRequest(), new WriteRequest(), new WriteRequest() }
            };

            await client.BatchWriteItemWithRetries(batches, configuration, logger);

            Assert.That(logger.InfoMessages, Is.Empty);
            Assert.That(logger.DebugMessages, Is.Empty);
        }

        class Logger : ILog
        {
            public void Debug(string message) => debugMessages.Add(message);

            public void Debug(string message, Exception exception) => debugMessages.Add($"{message}{exception.Message}");

            public void DebugFormat(string format, params object[] args) => debugMessages.Add(string.Format(format, args));

            public void Info(string message) => infoMessages.Add(message);

            public void Info(string message, Exception exception) => infoMessages.Add($"{message}{exception.Message}");

            public void InfoFormat(string format, params object[] args) => infoMessages.Add(string.Format(format, args));

            public void Warn(string message) => throw new NotImplementedException();

            public void Warn(string message, Exception exception) => throw new NotImplementedException();

            public void WarnFormat(string format, params object[] args) => throw new NotImplementedException();

            public void Error(string message) => throw new NotImplementedException();

            public void Error(string message, Exception exception) => throw new NotImplementedException();

            public void ErrorFormat(string format, params object[] args) => throw new NotImplementedException();

            public void Fatal(string message) => throw new NotImplementedException();

            public void Fatal(string message, Exception exception) => throw new NotImplementedException();

            public void FatalFormat(string format, params object[] args) => throw new NotImplementedException();

            public bool IsDebugEnabled { get; set; }
            public bool IsInfoEnabled { get; set; }
            public bool IsWarnEnabled { get; set; }
            public bool IsErrorEnabled { get; set; }
            public bool IsFatalEnabled { get; set; }

            public IReadOnlyCollection<string> DebugMessages => debugMessages;
            public IReadOnlyCollection<string> InfoMessages => infoMessages;

            List<string> debugMessages = new();
            List<string> infoMessages = new();
        }

        MockDynamoDBClient client;
        Logger logger;
        OutboxPersistenceConfiguration configuration;
    }
}