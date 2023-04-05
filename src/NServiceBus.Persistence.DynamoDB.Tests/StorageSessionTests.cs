namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System;
    using Amazon.DynamoDBv2.Model;
    using Extensibility;
    using NUnit.Framework;

    [TestFixture]
    public class StorageSessionTests
    {
        [Test]
        public void Should_throw_exception_when_adding_operations_after_dispose()
        {
            var session = new StorageSession(new MockDynamoDBClient(), new ContextBag());

            session.Dispose();

            Assert.Throws<ObjectDisposedException>(() => session.Add(new TransactWriteItem()));
            Assert.Throws<ObjectDisposedException>(() => session.AddRange(new[] { new TransactWriteItem(), new TransactWriteItem() }));
        }
    }
}