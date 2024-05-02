namespace NServiceBus.PersistenceTesting;

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
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

        AssertEverythingEmpty(read);
    }

    [Test]
    public async Task ShouldUpdate()
    {
        var memStreams = Enumerable.Range(0, 5)
            .Select(i => new MemoryStream(Encoding.UTF8.GetBytes($"Hello world {i}")))
            .ToArray();

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
            Floats = new HashSet<float>([1.234f, 5.678f]).ToImmutableHashSet(),
            Bytes = new SortedSet<byte>([0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64]).ToImmutableSortedSet(),
            Shorts = [1, 2, 3, 4],
            UShorts = [1, 2, 3, 4],
            Longs = new HashSet<long>([3147483647, 4147483647, 5147483647]).ToImmutableHashSet(),
            ULongs = new HashSet<ulong>([3147483647, 4147483647, 5147483647, 18446744073709551615]).ToImmutableSortedSet(),
            UInts = [2147483647, 4294967295],
            SBytes = [0x0F, 0x10],
            Decimals = new HashSet<decimal>([1.234m, 5.678m]).ToImmutableHashSet(),
            HashSetOfMemoryStreams = new HashSet<MemoryStream>(memStreams),
            ImmutableHashSetOfStreams = new HashSet<MemoryStream>(memStreams).ToImmutableHashSet(),
            HashSetOfString = ["a", "b", "c", "d"],
            SortedSetOfString = ["a", "b", "c", "d"],
            ImmutableHashSetOfString = new HashSet<string>(["a", "b", "c", "d"]).ToImmutableHashSet(),
            ImmutableSortedSetOfString = new SortedSet<string>(["a", "b", "c", "d"]).ToImmutableSortedSet(),

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
            read.Floats = ImmutableHashSet<float>.Empty;
            read.Bytes = ImmutableSortedSet<byte>.Empty;
            read.Shorts = [];
            read.UShorts = [];
            read.Longs = ImmutableHashSet<long>.Empty;
            read.ULongs = ImmutableSortedSet<ulong>.Empty;
            read.UInts = [];
            read.SBytes = [];
            read.Decimals = ImmutableHashSet<decimal>.Empty;
            read.HashSetOfMemoryStreams = [];
            read.ImmutableHashSetOfStreams = ImmutableHashSet<MemoryStream>.Empty;
            read.HashSetOfString = [];
            read.SortedSetOfString = [];
            read.ImmutableHashSetOfString = ImmutableHashSet<string>.Empty;
            read.ImmutableSortedSetOfString = ImmutableSortedSet<string>.Empty;

            await configuration.SagaStorage.Update(read, session, context);
            await session.CompleteAsync();
        }

        var read2 = await GetById<EmptyCollectionsSagaData>(sagaData.Id);
        Assert.That(read2, Is.Not.Null);
        Assert.That(read2.SomeId, Is.EqualTo(sagaData.SomeId));

        AssertEverythingEmpty(read2);
    }

    void AssertEverythingEmpty(EmptyCollectionsSagaData data) => Assert.Multiple(() =>
    {
        Assert.That(data.StringList, Is.Empty);
        Assert.That(data.StringArray, Is.Empty);
        Assert.That(data.RecordList, Is.Empty);
        Assert.That(data.RecordArray, Is.Empty);
        Assert.That(data.SimpleDict, Is.Empty);
        Assert.That(data.Ints, Is.Empty);
        Assert.That(data.Doubles, Is.Empty);
        Assert.That(data.Floats, Is.Empty);
        Assert.That(data.Bytes, Is.Empty);
        Assert.That(data.Shorts, Is.Empty);
        Assert.That(data.UShorts, Is.Empty);
        Assert.That(data.Longs, Is.Empty);
        Assert.That(data.ULongs, Is.Empty);
        Assert.That(data.UInts, Is.Empty);
        Assert.That(data.SBytes, Is.Empty);
        Assert.That(data.Decimals, Is.Empty);
        Assert.That(data.HashSetOfMemoryStreams, Is.Empty);
        Assert.That(data.ImmutableHashSetOfStreams, Is.Empty);
        Assert.That(data.HashSetOfString, Is.Empty);
        Assert.That(data.SortedSetOfString, Is.Empty);
        Assert.That(data.ImmutableHashSetOfString, Is.Empty);
        Assert.That(data.ImmutableSortedSetOfString, Is.Empty);
    });

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
        public ImmutableHashSet<float> Floats { get; set; } = ImmutableHashSet<float>.Empty;
        public ImmutableSortedSet<byte> Bytes { get; set; } = ImmutableSortedSet<byte>.Empty;
        public HashSet<short> Shorts { get; set; } = [];
        public SortedSet<ushort> UShorts { get; set; } = [];
        public ImmutableHashSet<long> Longs { get; set; } = ImmutableHashSet<long>.Empty;
        public ImmutableSortedSet<ulong> ULongs { get; set; } = ImmutableSortedSet<ulong>.Empty;
        public HashSet<uint> UInts { get; set; } = [];
        public SortedSet<sbyte> SBytes { get; set; } = [];
        public ImmutableHashSet<decimal> Decimals { get; set; } = ImmutableHashSet<decimal>.Empty;
#pragma warning disable PS0025 // It is a test
        public HashSet<MemoryStream> HashSetOfMemoryStreams { get; set; } = [];
        public ImmutableHashSet<MemoryStream> ImmutableHashSetOfStreams { get; set; } = ImmutableHashSet<MemoryStream>.Empty;
#pragma warning restore PS0025
        public HashSet<string> HashSetOfString { get; set; } = [];
        public SortedSet<string> SortedSetOfString { get; set; } = [];
        public ImmutableHashSet<string> ImmutableHashSetOfString { get; set; } = ImmutableHashSet<string>.Empty;
        public ImmutableSortedSet<string> ImmutableSortedSetOfString { get; set; } = ImmutableSortedSet<string>.Empty;
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