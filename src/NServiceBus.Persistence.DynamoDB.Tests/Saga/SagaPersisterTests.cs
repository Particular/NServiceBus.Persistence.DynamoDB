namespace NServiceBus.Persistence.DynamoDB.Tests.Saga
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2.Model;
    using Extensibility;
    using NUnit.Framework;
    using Sagas;

    [TestFixture]
    public class SagaPersisterTests
    {
        [Test]
        public async Task Save_should_add_to_session()
        {
            var persister = new SagaPersister(new SagaPersistenceConfiguration
            {
                TableName = "FakeTableName"
            }, null);
            var storageSession = new FakeStorageSession();

            var complexState = new ComplexStateSagaData
            {
                SomeId = Guid.NewGuid(),
                IntArray = new[] { 1, 2, 3, 4 },
                NullableDouble = 4.5d,
                ByteArray = new byte[] { 1 },
                NullableBool = true,
                NullableGuid = new Guid("3C623C1F-80AB-4036-86CA-C2020FAE2EFE"),
                NullableLong = 10,
                NullableInt = 10,
                ComplexData = new SomethingComplex { Data = "SomeData" }
            };

            await persister.Save(complexState, SagaCorrelationProperty.None, storageSession, new ContextBag(),
                CancellationToken.None);

            CollectionAssert.IsNotEmpty(storageSession.WriteItems);
        }

        public class ComplexStateSagaData : ContainSagaData
        {
            public Guid SomeId { get; set; }

            public int[] IntArray { get; set; }
            public double? NullableDouble { get; set; }

            public bool? NullableBool { get; set; }
            public int? NullableInt { get; set; }
            public Guid? NullableGuid { get; set; }
            public long? NullableLong { get; set; }
            public byte[] ByteArray { get; set; }

            public SomethingComplex ComplexData { get; set; }
        }

        public class SomethingComplex
        {
            public string Data { get; set; }
        }

        class FakeStorageSession : IDynamoDBStorageSession, ISynchronizedStorageSession
        {
            public List<TransactWriteItem> WriteItems { get; } = new List<TransactWriteItem>();
            public void Add(TransactWriteItem writeItem) => WriteItems.Add(writeItem);

            public void AddRange(IEnumerable<TransactWriteItem> writeItems) =>
                WriteItems.AddRange(writeItems);
        }
    }
}