namespace NServiceBus.PersistenceTesting
{
    using Sagas;
    using System.Threading.Tasks;
    using System;
    using NUnit.Framework;
    using System.Threading;

    public class When_concurrently_reading_saga_in_outbox_transaction : SagaPersisterTests
    {
        [Test]
        public async Task Should_lock_existing_saga_till_committed()
        {
            configuration.RequiresPessimisticConcurrencySupport();

            var saga = new TestSagaData { SomeId = Guid.NewGuid().ToString() };
            await SaveSaga(saga);

            var session1LockAcquired = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            TestSagaData session1Saga = null;
            int invocationCounter = 0;
            int session1Invocation = 0;
            var lockingSession = Task.Run(async () =>
            {
                var contextBag1 = configuration.GetContextBagForOutbox();
                using var outboxTransaction1 = await configuration.OutboxStorage.BeginTransaction(contextBag1);
                using (var synchronizedStorageSession = configuration.CreateStorageSession())
                {
                    await synchronizedStorageSession.TryOpen(outboxTransaction1, contextBag1);

                    session1Saga = await configuration.SagaStorage.Get<TestSagaData>(
                        nameof(TestSagaData.SomeId),
                        saga.SomeId, synchronizedStorageSession, contextBag1);

                    session1LockAcquired.SetResult(true); // attempt parallel read

                    await synchronizedStorageSession.CompleteAsync();
                }

                // give session 2 some time to read the same entry
                await Task.Delay(TimeSpan.FromTicks(configuration.SessionTimeout.GetValueOrDefault(TimeSpan.FromSeconds(1)).Ticks / 2));
                session1Invocation = Interlocked.Increment(ref invocationCounter);

                await outboxTransaction1.Commit();
            });

            TestSagaData session2Saga = null;
            int session2Invocation = 0;
            var blockedSession = Task.Run(async () =>
            {
                var contextBag2 = configuration.GetContextBagForOutbox();
                using var outboxTransaction2 = await configuration.OutboxStorage.BeginTransaction(contextBag2);
                using (var synchronizedStorageSession = configuration.CreateStorageSession())
                {
                    await synchronizedStorageSession.TryOpen(outboxTransaction2, contextBag2);

                    await session1LockAcquired.Task; //wait for session 1 to acquire lock before read

                    // this should be blocked by pessimistic concurrency until session 1 completed
                    session2Saga = await configuration.SagaStorage.Get<TestSagaData>(
                        nameof(TestSagaData.SomeId),
                        saga.SomeId, synchronizedStorageSession, contextBag2);
                    session2Invocation = Interlocked.Increment(ref invocationCounter);
                    await synchronizedStorageSession.CompleteAsync();
                }

                await outboxTransaction2.Commit();
            });

            await Task.WhenAll(lockingSession, blockedSession);

            Assert.IsNotNull(session1Saga);
            Assert.IsNotNull(session2Saga);
            Assert.Greater(session2Invocation, session1Invocation, "because session 2 should only be able to read after the transaction completed");
        }

        [Test]
        public async Task Should_lock_new_saga_till_committed()
        {
            configuration.RequiresPessimisticConcurrencySupport();

            var saga = new TestSagaData { SomeId = Guid.NewGuid().ToString() };

            var session1LockAcquired = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            TestSagaData session1Saga = null;
            int invocationCounter = 0;
            int session1Invocation = 0;
            var lockingSession = Task.Run(async () =>
            {
                var contextBag1 = configuration.GetContextBagForOutbox();
                using var outboxTransaction1 = await configuration.OutboxStorage.BeginTransaction(contextBag1);
                using (var synchronizedStorageSession = configuration.CreateStorageSession())
                {
                    await synchronizedStorageSession.TryOpen(outboxTransaction1, contextBag1);

                    session1Saga = await configuration.SagaStorage.Get<TestSagaData>(
                        nameof(TestSagaData.SomeId),
                        saga.SomeId, synchronizedStorageSession, contextBag1);

                    session1LockAcquired.SetResult(true); // attempt parallel read

                    await synchronizedStorageSession.CompleteAsync();
                }

                // give session 2 some time to read the same entry
                await Task.Delay(TimeSpan.FromTicks(configuration.SessionTimeout.GetValueOrDefault(TimeSpan.FromSeconds(1)).Ticks / 2));

                session1Invocation = Interlocked.Increment(ref invocationCounter);
                await outboxTransaction1.Commit();
            });

            TestSagaData session2Saga = null;
            int session2Invocation = 0;
            var blockedSession = Task.Run(async () =>
            {
                var contextBag2 = configuration.GetContextBagForOutbox();
                using var outboxTransaction2 = await configuration.OutboxStorage.BeginTransaction(contextBag2);
                using (var synchronizedStorageSession = configuration.CreateStorageSession())
                {
                    await synchronizedStorageSession.TryOpen(outboxTransaction2, contextBag2);

                    await session1LockAcquired.Task; //wait for session 1 to acquire lock before read

                    // this should be blocked by pessimistic concurrency until session 1 completed
                    session2Saga = await configuration.SagaStorage.Get<TestSagaData>(
                        nameof(TestSagaData.SomeId),
                        saga.SomeId, synchronizedStorageSession, contextBag2);
                    session2Invocation = Interlocked.Increment(ref invocationCounter);

                    await synchronizedStorageSession.CompleteAsync();
                }

                await outboxTransaction2.Commit();
            });

            await Task.WhenAll(lockingSession, blockedSession);

            Assert.IsNull(session1Saga);
            Assert.IsNull(session2Saga);
            Assert.Greater(session2Invocation, session1Invocation, "because session 2 should only be able to read after the transaction completed");
        }

        [Test]
        public async Task Should_unlock_existing_saga_when_uncommitted_transaction_disposed()
        {
            configuration.RequiresPessimisticConcurrencySupport();

            var saga = new TestSagaData { SomeId = Guid.NewGuid().ToString() };
            await SaveSaga(saga);

            var session1LockAcquired = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            TestSagaData session1Saga = null;
            int invocationCounter = 0;
            int session1Invocation = 0;
            var lockingSession = Task.Run(async () =>
            {
                var contextBag1 = configuration.GetContextBagForOutbox();
                using var outboxTransaction1 = await configuration.OutboxStorage.BeginTransaction(contextBag1);
                using (var synchronizedStorageSession = configuration.CreateStorageSession())
                {
                    await synchronizedStorageSession.TryOpen(outboxTransaction1, contextBag1);

                    session1Saga = await configuration.SagaStorage.Get<TestSagaData>(
                        nameof(TestSagaData.SomeId),
                        saga.SomeId, synchronizedStorageSession, contextBag1);

                    session1LockAcquired.SetResult(true); // attempt parallel read

                    await synchronizedStorageSession.CompleteAsync();
                }

                // give session 2 some time to read the same entry
                await Task.Delay(TimeSpan.FromTicks(configuration.SessionTimeout.GetValueOrDefault(TimeSpan.FromSeconds(1)).Ticks / 2));

                session1Invocation = Interlocked.Increment(ref invocationCounter);
                // do not commit, session is being disposed
            });

            TestSagaData session2Saga = null;
            int session2Invocation = 0;
            var blockedSession = Task.Run(async () =>
            {
                var contextBag2 = configuration.GetContextBagForOutbox();
                using var outboxTransaction2 = await configuration.OutboxStorage.BeginTransaction(contextBag2);
                using (var synchronizedStorageSession = configuration.CreateStorageSession())
                {
                    await synchronizedStorageSession.TryOpen(outboxTransaction2, contextBag2);

                    await session1LockAcquired.Task; //wait for session 1 to acquire lock before read

                    // this should be blocked by pessimistic concurrency until session 1 completed
                    session2Saga = await configuration.SagaStorage.Get<TestSagaData>(
                        nameof(TestSagaData.SomeId),
                        saga.SomeId, synchronizedStorageSession, contextBag2);
                    session2Invocation = Interlocked.Increment(ref invocationCounter);

                    await synchronizedStorageSession.CompleteAsync();
                }

                await outboxTransaction2.Commit();
            });

            await Task.WhenAll(lockingSession, blockedSession);

            Assert.IsNotNull(session1Saga);
            Assert.IsNotNull(session2Saga);
            Assert.Greater(session2Invocation, session1Invocation, "because session 2 should only be able to read after the transaction disposed");
        }

        [Test]
        public async Task Should_unlock_new_saga_when_uncommitted_transaction_disposed()
        {
            configuration.RequiresPessimisticConcurrencySupport();

            var saga = new TestSagaData { SomeId = Guid.NewGuid().ToString() };

            var session1LockAcquired = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            TestSagaData session1Saga = null;
            DateTime session1DisposeTime = default;
            var lockingSession = Task.Run(async () =>
            {
                var contextBag1 = configuration.GetContextBagForOutbox();
                using var outboxTransaction1 = await configuration.OutboxStorage.BeginTransaction(contextBag1);
                using (var synchronizedStorageSession = configuration.CreateStorageSession())
                {
                    await synchronizedStorageSession.TryOpen(outboxTransaction1, contextBag1);

                    session1Saga = await configuration.SagaStorage.Get<TestSagaData>(
                        nameof(TestSagaData.SomeId),
                        saga.SomeId, synchronizedStorageSession, contextBag1);

                    session1LockAcquired.SetResult(true); // attempt parallel read

                    await synchronizedStorageSession.CompleteAsync();
                }

                // give session 2 some time to read the same entry
                await Task.Delay(TimeSpan.FromTicks(configuration.SessionTimeout.GetValueOrDefault(TimeSpan.FromSeconds(1)).Ticks / 2));

                session1DisposeTime = DateTime.UtcNow;
                // do not commit, session is being disposed
            });

            TestSagaData session2Saga = null;
            DateTime session2ReadTimestamp = default;
            var lockedSession = Task.Run(async () =>
            {
                var contextBag2 = configuration.GetContextBagForOutbox();
                using var outboxTransaction2 = await configuration.OutboxStorage.BeginTransaction(contextBag2);
                using (var synchronizedStorageSession = configuration.CreateStorageSession())
                {
                    await synchronizedStorageSession.TryOpen(outboxTransaction2, contextBag2);

                    await session1LockAcquired.Task; //wait for session 1 to acquire lock before read

                    // this should be blocked by pessimistic concurrency until session 1 completed
                    session2Saga = await configuration.SagaStorage.Get<TestSagaData>(
                        nameof(TestSagaData.SomeId),
                        saga.SomeId, synchronizedStorageSession, contextBag2);
                    session2ReadTimestamp = DateTime.UtcNow;

                    await synchronizedStorageSession.CompleteAsync();
                }

                await outboxTransaction2.Commit();
            });

            await Task.WhenAll(lockingSession, lockedSession);

            Assert.IsNull(session1Saga);
            Assert.IsNull(session2Saga);
            Assert.Greater(session2ReadTimestamp, session1DisposeTime, "because session 2 should only be able to read after the transaction disposed");
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

        public When_concurrently_reading_saga_in_outbox_transaction(TestVariant param) : base(param)
        {
        }
    }
}