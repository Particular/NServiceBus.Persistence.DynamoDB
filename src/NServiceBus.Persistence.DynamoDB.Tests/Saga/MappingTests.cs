namespace NServiceBus.Persistence.DynamoDB.Tests.Saga
{
    using System;
    using NUnit.Framework;

    [TestFixture]
    public class MappingTests
    {
        [Test]
        public void FlatMapping()
        {
            var sagaData = new FlatSagaStateUsingSupportedTypes
            {
                Id = Guid.NewGuid(),
                OriginalMessageId = Guid.NewGuid().ToString(),
                SomeId = Guid.NewGuid(),
                SomeInt = 42,
                NullableInt = 42,
            };

            var attributes = Mapping.ToAttributes(sagaData);

            Assert.IsNotEmpty(attributes);
        }
        class FlatSagaStateUsingSupportedTypes : ContainSagaData
        {
            public Guid SomeId { get; set; }
            public int SomeInt { get; set; }
            public int? NullableInt { get; set; }
            //
            // public int[] IntArray { get; set; }
            // public double? NullableDouble { get; set; }
            //
            // public bool? NullableBool { get; set; }
            // public int? NullableInt { get; set; }
            // public Guid? NullableGuid { get; set; }
            // public long? NullableLong { get; set; }
            // public byte[] ByteArray { get; set; }
            //
            // public DateTime DateTime { get; set; }
            //
            // public DateTimeOffset DateTimeOffset { get; set; }
        }
    }
}