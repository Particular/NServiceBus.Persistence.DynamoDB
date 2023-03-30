namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2.Model;
    using NServiceBus.Testing;
    using NUnit.Framework;

    [TestFixture]
    public class TestableDynamoDBSynchronizedStorageSessionTests
    {
        [Test]
        public async Task Should_be_usable_with_testable_handler_context()
        {
            var handler = new Handler();
            var testableSession = new TestableDynamoDBSynchronizedStorageSession();
            await handler.Handle(new Handler.MyMessage(), new TestableMessageHandlerContext
            {
                SynchronizedStorageSession = testableSession
            });

            Assert.That(testableSession.TransactWriteItems, Has.Count.EqualTo(3));
        }

        class Handler : IHandleMessages<Handler.MyMessage>
        {
            public Task Handle(MyMessage message, IMessageHandlerContext context)
            {
                var session = context.SynchronizedStorageSession.DynamoDBPersistenceSession();

                session.Add(new TransactWriteItem());
                session.AddRange(new[] { new TransactWriteItem(), new TransactWriteItem() });

                return Task.CompletedTask;
            }


            public class MyMessage
            {
            }
        }

    }
}