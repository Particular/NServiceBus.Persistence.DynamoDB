namespace NServiceBus.PersistenceTesting
{
    using System.Threading.Tasks;
    using System;
    using System.Threading;
    using NUnit.Framework;
    using Sagas;

    public class When_failing_to_acquire_lock : SagaPersisterTests
    {
        [Test]
        public async Task Should_throw_timeout_exception()
        {
            configuration.RequiresPessimisticConcurrencySupport();

            var saga = new TestSagaData() { SomeId = Guid.NewGuid().ToString() };
            try
            {
                await SaveSaga(saga);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

            var session1Context = configuration.GetContextBagForSagaStorage();
            using (var session1 = configuration.CreateStorageSession())
            {
                await session1.Open(session1Context);

                // acquire lock
                var lockedSaga = await configuration.SagaStorage.Get<TestSagaData>(saga.Id, session1, session1Context);
                Assert.IsNotNull(lockedSaga);

                var session2Context = configuration.GetContextBagForSagaStorage();
                using (var session2 = configuration.CreateStorageSession())
                {
                    await session2.Open(session2Context);

                    // lock is still held by session 1
                    var exception = Assert.ThrowsAsync<TimeoutException>(() => configuration.SagaStorage.Get<TestSagaData>(saga.Id, session2, session2Context));
                }

                await session1.CompleteAsync();
            }
        }

        [Test]
        public async Task Should_throw_operationCanceledException_when_cancellation_before_timeout()
        {
            configuration.RequiresPessimisticConcurrencySupport();

            var saga = new TestSagaData() { SomeId = Guid.NewGuid().ToString() };
            await SaveSaga(saga);

            var session1Context = configuration.GetContextBagForSagaStorage();
            using (var session1 = configuration.CreateStorageSession())
            {
                await session1.Open(session1Context);

                // acquire lock
                var lockedSaga = await configuration.SagaStorage.Get<TestSagaData>(saga.Id, session1, session1Context);
                Assert.IsNotNull(lockedSaga);

                var session2Context = configuration.GetContextBagForSagaStorage();
                using (var session2 = configuration.CreateStorageSession())
                {
                    await session2.Open(session2Context);

                    var cancelledToken = new CancellationToken(true);
                    // lock is still held by session 1
                    var exception = Assert.ThrowsAsync<OperationCanceledException>(() => configuration.SagaStorage.Get<TestSagaData>(saga.Id, session2, session2Context, cancelledToken));
                }

                await session1.CompleteAsync();
            }
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