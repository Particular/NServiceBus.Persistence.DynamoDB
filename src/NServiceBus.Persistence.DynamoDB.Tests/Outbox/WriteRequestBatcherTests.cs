namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Amazon.DynamoDBv2.Model;
    using NUnit.Framework;

    [TestFixture]
    public class WriteRequestBatcherTests
    {
        [TestCaseSource(nameof(BatchingCases))]
        public void CorrectNumberOfBatches(int numberOfEntries, int expectedNumberOfBatches, int lastBatchContainsNumberOfEntries)
        {
            var writeRequests = Enumerable.Range(0, numberOfEntries).Select(x => new WriteRequest()).ToArray();

            var batches = WriteRequestBatcher.Batch(writeRequests);

            Assert.That(batches.Count, Is.EqualTo(expectedNumberOfBatches));
            for (int i = 0; i < expectedNumberOfBatches - 1; i++)
            {
                Assert.That(batches.ElementAt(i).Count, Is.EqualTo(25));
            }

            Assert.That(batches.LastOrDefault()?.Count, Is.EqualTo(lastBatchContainsNumberOfEntries));
        }

        public static IEnumerable<object[]> BatchingCases()
        {
            for (int i = 1; i < 100; i++)
            {
                int l = i % 25;
                yield return new object[] { i, (int)Math.Ceiling(i / 25d), l == 0 ? 25 : l };
            }
        }
    }
}