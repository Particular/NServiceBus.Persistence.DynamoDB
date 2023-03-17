namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2.Model;
    using Amazon.Runtime;
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

            Approver.Verify(logger);
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

            Approver.Verify(logger);
        }

        [Test]
        public async Task RetriesUnprocessedItemsInBatch()
        {
            var unprocessedWriteRequest1 = new WriteRequest();
            var unprocessedWriteRequest2 = new WriteRequest();

            var batches = new List<List<WriteRequest>>
            {
                new() { new WriteRequest(), unprocessedWriteRequest1, unprocessedWriteRequest2, new WriteRequest() }
            };

            int called = 0;
            client.BatchWriteRequestResponse = _ =>
            {
                called++;
                if (called == 1)
                {
                    return new BatchWriteItemResponse
                    {
                        UnprocessedItems = new Dictionary<string, List<WriteRequest>>
                        {
                            {
                                configuration.TableName,
                                new List<WriteRequest> { unprocessedWriteRequest1, unprocessedWriteRequest2 }
                            }
                        }
                    };
                }

                return new BatchWriteItemResponse();
            };

            await client.BatchWriteItemWithRetries(batches, configuration, logger, retryDelay: TimeSpan.FromMilliseconds(0));

            Assert.That(client.BatchWriteRequestsSent, Has.Count.EqualTo(2));
            Assert.That(client.BatchWriteRequestsSent, Has.One.Matches<BatchWriteItemRequest>(x => x.RequestItems[configuration.TableName].Count == 2));
            Assert.That(client.BatchWriteRequestsSent, Has.One.Matches<BatchWriteItemRequest>(x => x.RequestItems[configuration.TableName].Count == 4));

            var retriedBatch =
                client.BatchWriteRequestsSent.Single(x => x.RequestItems[configuration.TableName].Count == 2);

            Assert.That(retriedBatch.RequestItems[configuration.TableName], Is.EquivalentTo(new[] { unprocessedWriteRequest1, unprocessedWriteRequest2 }));
        }

        [Test]
        public async Task RetriesUnprocessedItemsInBatchUpToFiveTimesWithDelay()
        {
            var unprocessedWriteRequest1 = new WriteRequest();
            var unprocessedWriteRequest2 = new WriteRequest();

            var batches = new List<List<WriteRequest>>
            {
                new() { new WriteRequest(), unprocessedWriteRequest1, unprocessedWriteRequest2, new WriteRequest() }
            };

            int called = 0;
            client.BatchWriteRequestResponse = _ =>
            {
                called++;
                if (called < 6)
                {
                    return new BatchWriteItemResponse
                    {
                        UnprocessedItems = new Dictionary<string, List<WriteRequest>>
                        {
                            {
                                configuration.TableName,
                                new List<WriteRequest> { unprocessedWriteRequest1, unprocessedWriteRequest2 }
                            }
                        }
                    };
                }

                return new BatchWriteItemResponse();
            };

            await client.BatchWriteItemWithRetries(batches, configuration, logger, delayOnFailure: (_, _) => Task.CompletedTask, retryDelay: TimeSpan.FromMilliseconds(200));

            Assert.That(client.BatchWriteRequestsSent, Has.Count.EqualTo(6));
            Assert.That(client.BatchWriteRequestsSent, Has.Exactly(5).Matches<BatchWriteItemRequest>(x => x.RequestItems[configuration.TableName].Count == 2));
            Assert.That(client.BatchWriteRequestsSent, Has.One.Matches<BatchWriteItemRequest>(x => x.RequestItems[configuration.TableName].Count == 4));

            var retriedBatches =
                client.BatchWriteRequestsSent.Where(x => x.RequestItems[configuration.TableName].Count == 2)
                    .ToArray();

            Assert.That(retriedBatches, Has.Length.EqualTo(5));

            foreach (var batch in retriedBatches)
            {
                Assert.That(batch.RequestItems[configuration.TableName], Is.EquivalentTo(new[] { unprocessedWriteRequest1, unprocessedWriteRequest2 }));
            }

            Approver.Verify(logger);
        }

        [Test]
        public async Task GivesUpRetryingUnprocessedItemsAfterFiveAttempts()
        {
            var unprocessedWriteRequest1 = new WriteRequest();
            var unprocessedWriteRequest2 = new WriteRequest();

            var batches = new List<List<WriteRequest>>
            {
                new() { new WriteRequest(), unprocessedWriteRequest1, unprocessedWriteRequest2, new WriteRequest() }
            };

            client.BatchWriteRequestResponse = _ => new BatchWriteItemResponse
            {
                UnprocessedItems = new Dictionary<string, List<WriteRequest>>
                {
                    {
                        configuration.TableName,
                        new List<WriteRequest> { unprocessedWriteRequest1, unprocessedWriteRequest2 }
                    }
                }
            };

            await client.BatchWriteItemWithRetries(batches, configuration, logger, delayOnFailure: (_, _) => Task.CompletedTask, retryDelay: TimeSpan.FromMilliseconds(200));

            Approver.Verify(logger);
        }

        [Test]
        public async Task RetriesUnprocessedItemsOnUnsuccessfulBatches()
        {
            logger.IsDebugEnabled = true;

            string unprocessedWriteRequest1Key = Guid.NewGuid().ToString();
            var unprocessedWriteRequest1 = new WriteRequest(new DeleteRequest(new Dictionary<string, AttributeValue>()
            {
                { configuration.PartitionKeyName, new("PK1") },
                { configuration.SortKeyName, new("SK1") },
                { "ShouldFail", new AttributeValue(unprocessedWriteRequest1Key) }
            }));
            string unprocessedWriteRequest2Key = Guid.NewGuid().ToString();
            var unprocessedWriteRequest2 = new WriteRequest(new DeleteRequest(new Dictionary<string, AttributeValue>()
            {
                { configuration.PartitionKeyName, new("PK2") },
                { configuration.SortKeyName, new("SK2") },
                { "ShouldFail", new AttributeValue(unprocessedWriteRequest2Key) }
            }));

            var batches = new List<List<WriteRequest>>
            {
                new() { new WriteRequest(), new WriteRequest(), new WriteRequest(), new WriteRequest() },
                new() { new WriteRequest(), unprocessedWriteRequest1, new WriteRequest(), new WriteRequest() },
                new() { new WriteRequest(), new WriteRequest(), new WriteRequest(), new WriteRequest() },
                new() { new WriteRequest(), unprocessedWriteRequest2, new WriteRequest(), new WriteRequest() },
            };

            var calledMap = new Dictionary<string, int>
            {
                {unprocessedWriteRequest1Key, 0 },
                {unprocessedWriteRequest2Key, 0 },
            };
            client.BatchWriteRequestResponse = req =>
            {
                var shouldFail = req.RequestItems[configuration.TableName]
                    .Where(x => x.DeleteRequest is { } del && del.Key.ContainsKey("ShouldFail"))
                    .ToList();
                if (shouldFail.Count > 0 && calledMap[shouldFail[0].DeleteRequest.Key["ShouldFail"].S]++ == 0)
                {
                    return new BatchWriteItemResponse
                    {
                        UnprocessedItems = new Dictionary<string, List<WriteRequest>>
                        {
                            { configuration.TableName, shouldFail }
                        }
                    };
                }

                return new BatchWriteItemResponse();
            };

            await client.BatchWriteItemWithRetries(batches, configuration, logger, delayOnFailure: (_, _) => Task.CompletedTask, retryDelay: TimeSpan.FromMilliseconds(200));

            Assert.That(client.BatchWriteRequestsSent, Has.Count.EqualTo(6));
            Assert.That(client.BatchWriteRequestsSent, Has.Exactly(2).Matches<BatchWriteItemRequest>(x => x.RequestItems[configuration.TableName].Count == 1));
            Assert.That(client.BatchWriteRequestsSent, Has.Exactly(4).Matches<BatchWriteItemRequest>(x => x.RequestItems[configuration.TableName].Count == 4));

            var retriedBatches =
                client.BatchWriteRequestsSent.Where(x => x.RequestItems[configuration.TableName].Count == 1)
                    .ToArray();

            Assert.That(retriedBatches, Has.Length.EqualTo(2));
            Assert.That(retriedBatches.ElementAt(0).RequestItems[configuration.TableName].Single(), Is.EqualTo(unprocessedWriteRequest1));
            Assert.That(retriedBatches.ElementAt(1).RequestItems[configuration.TableName].Single(), Is.EqualTo(unprocessedWriteRequest2));

            Approver.Verify(logger);
        }

        [Test]
        public async Task OnThrottlingRetriesWholeBatch()
        {
            var batch = new List<WriteRequest> { new WriteRequest(), new WriteRequest(), new WriteRequest(), new WriteRequest() };
            var batches = new List<List<WriteRequest>>
            {
                batch
            };

            int called = 0;
            client.BatchWriteRequestResponse = _ =>
            {
                called++;
                if (called == 1)
                {
                    throw new ProvisionedThroughputExceededException("");
                }

                return new BatchWriteItemResponse();
            };

            await client.BatchWriteItemWithRetries(batches, configuration, logger, retryDelay: TimeSpan.FromMilliseconds(0));

            Assert.That(client.BatchWriteRequestsSent, Has.Count.EqualTo(2));
            Assert.That(client.BatchWriteRequestsSent, Has.All.Matches<BatchWriteItemRequest>(x => x.RequestItems[configuration.TableName].Count == 4));

            var retriedBatches =
                client.BatchWriteRequestsSent.Where(x => x.RequestItems[configuration.TableName].Count == 4)
                    .ToArray();

            Assert.That(retriedBatches, Has.Length.EqualTo(2));

            foreach (var retriedBatch in retriedBatches)
            {
                Assert.That(retriedBatch.RequestItems[configuration.TableName], Is.EqualTo(batch));
            }
        }

        [Test]
        public async Task OnThrottlingRetriesWholeUpToFiveTimesWithDelay()
        {
            var batch = new List<WriteRequest> { new WriteRequest(), new WriteRequest(), new WriteRequest(), new WriteRequest() };
            var batches = new List<List<WriteRequest>>
            {
                batch
            };

            int called = 0;
            client.BatchWriteRequestResponse = _ =>
            {
                called++;
                if (called < 6)
                {
                    throw new ProvisionedThroughputExceededException("");
                }

                return new BatchWriteItemResponse();
            };

            await client.BatchWriteItemWithRetries(batches, configuration, logger, delayOnFailure: (_, _) => Task.CompletedTask, retryDelay: TimeSpan.FromMilliseconds(200));

            Assert.That(client.BatchWriteRequestsSent, Has.Count.EqualTo(6));
            Assert.That(client.BatchWriteRequestsSent, Has.All.Matches<BatchWriteItemRequest>(x => x.RequestItems[configuration.TableName].Count == 4));

            var retriedBatches =
                client.BatchWriteRequestsSent.Skip(1).Where(x => x.RequestItems[configuration.TableName].Count == 4)
                    .ToArray();

            Assert.That(retriedBatches, Has.Length.EqualTo(5));

            foreach (var retriedBatch in retriedBatches)
            {
                Assert.That(retriedBatch.RequestItems[configuration.TableName], Is.EqualTo(batch));
            }

            Approver.Verify(logger);
        }

        [Test]
        public async Task OnThrottlingGivesUpRetryingUnprocessedItemsAfterFiveAttempts()
        {
            var batches = new List<List<WriteRequest>>
            {
                new() { new WriteRequest(), new WriteRequest(), new WriteRequest(), new WriteRequest() }
            };

            client.BatchWriteRequestResponse = _ => throw new ProvisionedThroughputExceededException("");

            await client.BatchWriteItemWithRetries(batches, configuration, logger, delayOnFailure: (_, _) => Task.CompletedTask, retryDelay: TimeSpan.FromMilliseconds(200));

            Approver.Verify(logger);
        }

        [Test]
        public void DoesNotRetryOnOtherExceptions()
        {
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

            client.BatchWriteRequestResponse = _ => throw new AmazonServiceException("");

            Assert.ThrowsAsync<AmazonServiceException>(async () =>
                await client.BatchWriteItemWithRetries(batches, configuration, logger));

            Assert.That(client.BatchWriteRequestsSent, Has.Count.EqualTo(2));
            Approver.Verify(logger);
        }

        class Logger : ILog
        {
            public void Debug(string message) => debugMessages.Add(message);

            public void Debug(string message, Exception exception) => debugMessages.Add($"{message}{exception.Message}");

            public void DebugFormat(string format, params object[] args) => debugMessages.Add(string.Format(format, args));

            public void Info(string message) => infoMessages.Add(message);

            public void Info(string message, Exception exception) => infoMessages.Add($"{message}{exception.Message}");

            public void InfoFormat(string format, params object[] args) => infoMessages.Add(string.Format(format, args));

            public void Warn(string message) => warnMessages.Add(message);

            public void Warn(string message, Exception exception) => warnMessages.Add($"{message}{exception.Message}");

            public void WarnFormat(string format, params object[] args) => warnMessages.Add(string.Format(format, args));

            public void Error(string message) => errorMessages.Add(message);

            public void Error(string message, Exception exception) => errorMessages.Add($"{message}{exception.Message}");

            public void ErrorFormat(string format, params object[] args) => errorMessages.Add(string.Format(format, args));

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
            public IReadOnlyCollection<string> WarnMessages => warnMessages;
            public IReadOnlyCollection<string> ErrorMessages => errorMessages;

            List<string> debugMessages = new();
            List<string> infoMessages = new();
            List<string> warnMessages = new();
            List<string> errorMessages = new();
        }

        MockDynamoDBClient client;
        Logger logger;
        OutboxPersistenceConfiguration configuration;
    }
}