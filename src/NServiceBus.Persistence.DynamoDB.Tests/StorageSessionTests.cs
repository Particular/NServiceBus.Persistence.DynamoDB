namespace NServiceBus.Persistence.DynamoDB.Tests;

using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Extensibility;
using NUnit.Framework;

[TestFixture]
public class StorageSessionTests
{
    [Test]
    public void Dispose_should_call_cleanup_actions()
    {
        var action1 = new MockLockCleanup();
        var action2 = new MockLockCleanup();

        var session = new StorageSession(new MockDynamoDBClient(), new ContextBag());
        session.AddToBeExecutedWhenSessionDisposes(action1);
        session.AddToBeExecutedWhenSessionDisposes(action2);

        // the cleanup happens async, but because we're never actually move away from sync code paths, we can immediately assert after calling dispose
        session.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(action1.WasCalled, Is.True);
            Assert.That(action2.WasCalled, Is.True);
        });
    }

    [Test]
    public void Dispose_should_call_cleanup_actions_only_once()
    {
        var action = new MockLockCleanup();
        var session = new StorageSession(new MockDynamoDBClient(), new ContextBag());
        session.AddToBeExecutedWhenSessionDisposes(action);

        session.Dispose();
        session.Dispose();

        Assert.That(action.NumberOfTimesCalled, Is.EqualTo(1));
    }

    [Test]
    public void Add_should_throw_exception_when_adding_operations_after_disposal()
    {
        var session = new StorageSession(new MockDynamoDBClient(), new ContextBag());

        session.Dispose();

        Assert.Throws<ObjectDisposedException>(() => session.Add(new TransactWriteItem()));
        Assert.Throws<ObjectDisposedException>(() => session.AddRange(new[] { new TransactWriteItem(), new TransactWriteItem() }));
    }

    class MockLockCleanup : ILockCleanup
    {
        public Guid Id { get; } = Guid.NewGuid();
        public bool NoLongerNecessaryWhenSessionCommitted { get; set; }
        public bool WasCalled => NumberOfTimesCalled > 0;
        public int NumberOfTimesCalled { get; private set; }

        public Task Cleanup(IAmazonDynamoDB client, CancellationToken cancellationToken = default)
        {
            NumberOfTimesCalled++;
            return Task.CompletedTask;
        }
    }
}