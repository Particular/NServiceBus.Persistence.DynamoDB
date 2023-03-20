namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System.IO;
    using System.Text;
    using NUnit.Framework;

    [TestFixture]
    public class DataSerializerTests
    {
        [Test]
        public void Should_roundtrip_streams()
        {
            var classWithMemoryStream = new ClassWithMemoryStream
            {
                SomeStream = new MemoryStream(Encoding.UTF8.GetBytes("Hello World"))
            };

            var attributes = DataSerializer.Serialize(classWithMemoryStream);

            var deserialized = DataSerializer.Deserialize<ClassWithMemoryStream>(attributes);

            CollectionAssert.AreEquivalent(classWithMemoryStream.SomeStream.ToArray(), deserialized.SomeStream.ToArray());
        }

        class ClassWithMemoryStream
        {
            public MemoryStream SomeStream { get; set; }
        }
    }
}