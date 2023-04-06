namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2.Model;
    using Extensibility;
    using NUnit.Framework;

    [TestFixture]
    public class StorageSessionTests
    {
        [Test]
        public void Dispose_should_call_cleanup_actions()
        {
            var calledCleanupActions = new List<Guid>();
            Task CleanupAction(Guid guid)
            {
                calledCleanupActions.Add(guid);
                return Task.CompletedTask;
            }

            var action1 = Guid.NewGuid();
            var action2 = Guid.NewGuid();

            var session = new StorageSession(new MockDynamoDBClient(), new ContextBag());
            session.CleanupActions[action1] = (_, _) => CleanupAction(action1);
            session.CleanupActions[action2] = (_, _) => CleanupAction(action2);

            // the cleanup happens async, but because we're never actually move away from sync code paths, we can immediately assert after calling dispose
            session.Dispose();

            Assert.That(calledCleanupActions, Has.Count.EqualTo(2));
            Assert.That(calledCleanupActions, Contains.Item(action1).And.Contains(action2));
        }

        [Test]
        public void Dispose_should_call_cleanup_actions_only_once()
        {
            int counter = 0;
            var session = new StorageSession(new MockDynamoDBClient(), new ContextBag());
            session.CleanupActions[Guid.NewGuid()] = (_, _) =>
            {
                counter++;
                return Task.CompletedTask;
            };

            session.Dispose();
            session.Dispose();

            Assert.AreEqual(1, counter);
        }

        [Test]
        public void Add_should_throw_exception_when_adding_operations_after_disposal()
        {
            var session = new StorageSession(new MockDynamoDBClient(), new ContextBag());

            session.Dispose();

            Assert.Throws<ObjectDisposedException>(() => session.Add(new TransactWriteItem()));
            Assert.Throws<ObjectDisposedException>(() => session.AddRange(new[] { new TransactWriteItem(), new TransactWriteItem() }));
        }
    }
}