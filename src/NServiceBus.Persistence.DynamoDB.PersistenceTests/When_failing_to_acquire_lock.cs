namespace NServiceBus.PersistenceTesting
{
    using System.Threading.Tasks;
    using System;
    using System.Threading;
    using NUnit.Framework;
    using Sagas;

    // These tests are very similar to When_concurrent_update_exceed_lock_request_timeout_pessimistic, but they are more specific in regards to the expected exception type
    public class When_failing_to_acquire_lock : SagaPersisterTests
    {
        [Test]
        public async Task Should_throw_timeout_exception()
        {
            configuration.RequiresPessimisticConcurrencySupport();

            var saga = new TestSagaData() { SomeId = Guid.NewGuid().ToString() };
            await SaveSaga(saga);

            var lockingSessionContext = configuration.GetContextBagForSagaStorage();
            using var lockingSession = configuration.CreateStorageSession();
            await lockingSession.Open(lockingSessionContext);

            // acquire lock
            var lockedSaga = await configuration.SagaStorage.Get<TestSagaData>(saga.Id, lockingSession, lockingSessionContext);
            Assert.IsNotNull(lockedSaga);

            var blockedSessionContext = configuration.GetContextBagForSagaStorage();
            using (var blockedSession = configuration.CreateStorageSession())
            {
                await blockedSession.Open(blockedSessionContext);

                // lock is still held by session 1
                var exception = Assert.ThrowsAsync<TimeoutException>(() => configuration.SagaStorage.Get<TestSagaData>(saga.Id, blockedSession, blockedSessionContext));
            }

            await lockingSession.CompleteAsync();
        }

        [Test]
        public async Task Should_throw_operationCanceledException_when_cancellation_before_timeout()
        {
            configuration.RequiresPessimisticConcurrencySupport();

            var saga = new TestSagaData() { SomeId = Guid.NewGuid().ToString() };
            await SaveSaga(saga);

            var session1Context = configuration.GetContextBagForSagaStorage();
            using var lockingSession = configuration.CreateStorageSession();
            await lockingSession.Open(session1Context);

            // acquire lock
            var lockedSaga = await configuration.SagaStorage.Get<TestSagaData>(saga.Id, lockingSession, session1Context);
            Assert.IsNotNull(lockedSaga);

            var session2Context = configuration.GetContextBagForSagaStorage();
            using (var blockedSession = configuration.CreateStorageSession())
            {
                await blockedSession.Open(session2Context);

                var cancelTokenAfter = TimeSpan.FromTicks(configuration.SessionTimeout.GetValueOrDefault(TimeSpan.FromSeconds(1)).Ticks / 4);
                using var pipelineCancellationTokenSource = new CancellationTokenSource(cancelTokenAfter);
                // lock is still held by session 1
                Assert.CatchAsync<OperationCanceledException>(() => configuration.SagaStorage.Get<TestSagaData>(saga.Id, blockedSession, session2Context, pipelineCancellationTokenSource.Token));
            }

            await lockingSession.CompleteAsync();
        }

        public class TestSaga : Saga<TestSagaData>, IAmStartedByMessages<StartTestSagaMessage>
        {
            protected override void ConfigureHowToFindSaga(SagaPropertyMapper<TestSagaData> mapper) => mapper.ConfigureMapping<StartTestSagaMessage>(m => m.SomeId).ToSaga(s => s.SomeId);

            public Task Handle(StartTestSagaMessage message, IMessageHandlerContext context) => throw new NotImplementedException();
        }

        public class TestSagaData : ContainSagaData
        {
            public string SomeId { get; set; } = "Test";
        }

        public class StartTestSagaMessage : IMessage
        {
            public string SomeId { get; set; }
        }

        public When_failing_to_acquire_lock(TestVariant param) : base(param)
        {
        }
    }
}