namespace NServiceBus.PersistenceTesting
{
    using System;
    using System.Threading.Tasks;
    using NServiceBus.PersistenceTesting.Sagas;
    using NUnit.Framework;

    public class When_concurrently_creating_saga_in_outbox_transaction : SagaPersisterTests
    {
        [Test]
        public async Task Should_lock_saga_until_transaction_committed()
        {
            configuration.RequiresPessimisticConcurrencySupport();

            var saga1 = new TestSagaData { SomeId = Guid.NewGuid().ToString() };

            var session1LockAcquired = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            var lockingSession = Task.Run(async () =>
            {
                var contextBag1 = configuration.GetContextBagForOutbox();
                using var outboxTransaction1 = await configuration.OutboxStorage.BeginTransaction(contextBag1);
                using (var synchronizedStorageSession = configuration.CreateStorageSession())
                {
                    await synchronizedStorageSession.TryOpen(outboxTransaction1, contextBag1);

                    var readBeforeCreate = await configuration.SagaStorage.Get<TestSagaData>(
                        nameof(TestSagaData.SomeId),
                        saga1.SomeId, synchronizedStorageSession, contextBag1);
                    Assert.IsNull(readBeforeCreate);
                    session1LockAcquired.SetResult(true); // attempt parallel read


                    await SaveSagaWithSession(saga1, synchronizedStorageSession, contextBag1);

                    await synchronizedStorageSession.CompleteAsync();
                }

                // give session 2 some time to read the same entry
                await Task.Delay(TimeSpan.FromTicks(configuration.SessionTimeout.GetValueOrDefault(TimeSpan.FromSeconds(1)).Ticks / 2));

                await outboxTransaction1.Commit();
            });

            var blockedSession = Task.Run(async () =>
            {
                var contextBag2 = configuration.GetContextBagForOutbox();
                using var outboxTransaction2 = await configuration.OutboxStorage.BeginTransaction(contextBag2);
                using var synchronizedStorageSession = configuration.CreateStorageSession();
                await synchronizedStorageSession.TryOpen(outboxTransaction2, contextBag2);

                await session1LockAcquired.Task; //wait for session 1 to acquire lock before read

                // this should be blocked by pessimistic concurrency until session 1 completed
                var session2Read = await configuration.SagaStorage.Get<TestSagaData>(
                    nameof(TestSagaData.SomeId),
                    saga1.SomeId, synchronizedStorageSession, contextBag2);
                // after session 1 completed, we should read the created saga
                Assert.NotNull(session2Read);
            });

            await Task.WhenAll(lockingSession, blockedSession);
        }

        [Test]
        public async Task Should_lock_saga_until_transaction_disposed()
        {
            configuration.RequiresPessimisticConcurrencySupport();

            var saga1 = new TestSagaData { SomeId = Guid.NewGuid().ToString() };

            var session1LockAcquired = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            var lockingSession = Task.Run(async () =>
            {
                var contextBag1 = configuration.GetContextBagForOutbox();
                using var outboxTransaction1 = await configuration.OutboxStorage.BeginTransaction(contextBag1);
                using (var synchronizedStorageSession = configuration.CreateStorageSession())
                {
                    await synchronizedStorageSession.TryOpen(outboxTransaction1, contextBag1);

                    var readBeforeCreate = await configuration.SagaStorage.Get<TestSagaData>(
                        nameof(TestSagaData.SomeId),
                        saga1.SomeId, synchronizedStorageSession, contextBag1);
                    Assert.IsNull(readBeforeCreate);
                    session1LockAcquired.SetResult(true); // attempt parallel read


                    await SaveSagaWithSession(saga1, synchronizedStorageSession, contextBag1);

                    await synchronizedStorageSession.CompleteAsync();
                }

                // give session 2 some time to read the same entry
                await Task.Delay(TimeSpan.FromTicks(configuration.SessionTimeout.GetValueOrDefault(TimeSpan.FromSeconds(1)).Ticks / 2));

                // do not commit transaction
            });

            var blockedSession = Task.Run(async () =>
            {
                var contextBag2 = configuration.GetContextBagForOutbox();
                using var outboxTransaction2 = await configuration.OutboxStorage.BeginTransaction(contextBag2);
                using var synchronizedStorageSession = configuration.CreateStorageSession();
                await synchronizedStorageSession.TryOpen(outboxTransaction2, contextBag2);

                await session1LockAcquired.Task; //wait for session 1 to acquire lock before read

                // this should be blocked by pessimistic concurrency until session 1 completed
                var session2Read = await configuration.SagaStorage.Get<TestSagaData>(
                    nameof(TestSagaData.SomeId),
                    saga1.SomeId, synchronizedStorageSession, contextBag2);
                // after session 1 completed, we should read the created saga
                Assert.IsNull(session2Read);
            });

            await Task.WhenAll(lockingSession, blockedSession);
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

        public When_concurrently_creating_saga_in_outbox_transaction(TestVariant param) : base(param)
        {
        }
    }
}