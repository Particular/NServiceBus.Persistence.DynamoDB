namespace NServiceBus.PersistenceTesting;

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Sagas;

public class When_saving_saga_with_empty_list : SagaPersisterTests
{
    [Test]
    public async Task ShouldSave()
    {
        var sagaData = new EmptyCollectionsSagaData { SomeId = Guid.NewGuid().ToString() };
        await SaveSaga(sagaData);

        var read = await GetById<EmptyCollectionsSagaData>(sagaData.Id);
        Assert.That(read, Is.Not.Null);
        Assert.That(read.SomeId, Is.EqualTo(sagaData.SomeId));

        Assert.Multiple(() =>
        {
            Assert.That(read.StringList, Is.Empty);
            Assert.That(read.StringArray, Is.Empty);
            Assert.That(read.RecordList, Is.Empty);
            Assert.That(read.RecordArray, Is.Empty);
            Assert.That(read.SimpleDict, Is.Empty);
        });
    }

    [Test]
    public async Task ShouldUpdate()
    {
        var sagaData = new EmptyCollectionsSagaData
        {
            SomeId = Guid.NewGuid().ToString(),
            StringList = ["a", "b"],
            StringArray = ["c", "d"],
            RecordList = [new("e", 1.2), new("f", 3.4)],
            RecordArray = [new("g", 5.6), new("h", 7.8)],
            SimpleDict = new Dictionary<string, int>
            {
                ["i"] = 9,
                ["j"] = 10,
            },
            Ints = [11, 12],
            Doubles = [13.4, 15.6],
        };
        await SaveSaga(sagaData);

        var context = configuration.GetContextBagForSagaStorage();
        using (var session = configuration.CreateStorageSession())
        {
            await session.Open(context);
            var read = await configuration.SagaStorage.Get<EmptyCollectionsSagaData>("SomeId", sagaData.SomeId, session, context);

            Assert.That(read, Is.Not.Null);
            Assert.That(read.SomeId, Is.EqualTo(sagaData.SomeId));

            read.StringList = [];
            read.StringArray = [];
            read.RecordList = [];
            read.RecordArray = [];
            read.SimpleDict = [];
            read.Ints = [];
            read.Doubles = [];
            read.Floats = [];
            read.Bytes = [];
            read.Shorts = [];
            read.UShorts = [];
            read.Longs = [];
            read.ULongs = [];
            read.UInts = [];
            read.SBytes = [];
            read.Decimals = [];
            read.HashSetOfMemoryStreams = [];
            read.ImmutableHashSetOfStreams = [];
            read.HashSetOfString = [];
            read.SortedSetOfString = [];
            read.ImmutableHashSetOfString = [];
            read.ImmutableSortedSetOfString = [];

            await configuration.SagaStorage.Update(read, session, context);
            await session.CompleteAsync();
        }

        var read2 = await GetById<EmptyCollectionsSagaData>(sagaData.Id);
        Assert.That(read2, Is.Not.Null);
        Assert.That(read2.SomeId, Is.EqualTo(sagaData.SomeId));

        Assert.Multiple(() =>
        {
            Assert.That(read2.StringList, Is.Empty);
            Assert.That(read2.StringArray, Is.Empty);
            Assert.That(read2.RecordList, Is.Empty);
            Assert.That(read2.RecordArray, Is.Empty);
            Assert.That(read2.SimpleDict, Is.Empty);
            Assert.That(read2.Ints, Is.Empty);
            Assert.That(read2.Doubles, Is.Empty);
            Assert.That(read2.Floats, Is.Empty);
            Assert.That(read2.Bytes, Is.Empty);
            Assert.That(read2.Shorts, Is.Empty);
            Assert.That(read2.UShorts, Is.Empty);
            Assert.That(read2.Longs, Is.Empty);
            Assert.That(read2.ULongs, Is.Empty);
            Assert.That(read2.UInts, Is.Empty);
            Assert.That(read2.SBytes, Is.Empty);
            Assert.That(read2.Decimals, Is.Empty);
            Assert.That(read2.HashSetOfMemoryStreams, Is.Empty);
            Assert.That(read2.ImmutableHashSetOfStreams, Is.Empty);
            Assert.That(read2.HashSetOfString, Is.Empty);
            Assert.That(read2.SortedSetOfString, Is.Empty);
            Assert.That(read2.ImmutableHashSetOfString, Is.Empty);
            Assert.That(read2.ImmutableSortedSetOfString, Is.Empty);
        });
    }

    // Even though not used, need this class so saga data will get picked up by mapper
    public class EmptyCollectionsSaga : Saga<EmptyCollectionsSagaData>,
        IAmStartedByMessages<TestMessage>
    {
        public Task Handle(TestMessage message, IMessageHandlerContext context) => throw new NotImplementedException();

        protected override void ConfigureHowToFindSaga(SagaPropertyMapper<EmptyCollectionsSagaData> mapper)
        {
            mapper.MapSaga(s => s.SomeId)
                .ToMessage<TestMessage>(m => m.SomeId);
        }
    }

    public class EmptyCollectionsSagaData : ContainSagaData
    {
        public string SomeId { get; set; } = "Test";

        public List<string> StringList { get; set; } = [];
        public string[] StringArray { get; set; } = [];
        public List<SimpleType> RecordList { get; set; } = [];
        public SimpleType[] RecordArray { get; set; } = [];
        public Dictionary<string, int> SimpleDict { get; set; } = [];
        public HashSet<int> Ints { get; set; } = [];
        public SortedSet<double> Doubles { get; set; } = [];
        public ImmutableHashSet<float> Floats { get; set; } = [];
        public ImmutableSortedSet<byte> Bytes { get; set; } = [];
        public HashSet<short> Shorts { get; set; } = [];
        public SortedSet<ushort> UShorts { get; set; } = [];
        public ImmutableHashSet<long> Longs { get; set; } = [];
        public ImmutableSortedSet<ulong> ULongs { get; set; } = [];
        public HashSet<uint> UInts { get; set; } = [];
        public SortedSet<sbyte> SBytes { get; set; } = [];
        public ImmutableHashSet<decimal> Decimals { get; set; } = [];
#pragma warning disable PS0025 // It is a test
        public HashSet<MemoryStream> HashSetOfMemoryStreams { get; set; } = [];
        public ImmutableHashSet<MemoryStream> ImmutableHashSetOfStreams { get; set; } = [];
#pragma warning restore PS0025
        public HashSet<string> HashSetOfString { get; set; } = [];
        public SortedSet<string> SortedSetOfString { get; set; } = [];
        public ImmutableHashSet<string> ImmutableHashSetOfString { get; set; } = [];
        public ImmutableSortedSet<string> ImmutableSortedSetOfString { get; set; } = [];
    }

    public record SimpleType(string Id, double Value);

    public class TestMessage : ICommand
    {
        public string SomeId { get; set; }
    }

    public When_saving_saga_with_empty_list(TestVariant param) : base(param)
    {
    }
}