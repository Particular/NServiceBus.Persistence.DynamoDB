namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System;
    using System.Reflection;
    using Extensibility;
    using NUnit.Framework;
    using Sagas;

    [TestFixture]
    public class SagaIdGeneratorTests
    {
        [Test]
        public void Should_generate_deterministic_id()
        {
            var generator = new SagaIdGenerator();
            var sagaMetadata = CreateSagaMetadata<SagaTypeA, SagaDataTypeA>();

            var id1 = generator.Generate(new SagaIdGeneratorContext(
                new SagaCorrelationProperty("correlation property name", "correlation property value"),
                sagaMetadata, new ContextBag()));
            var id2 = generator.Generate(new SagaIdGeneratorContext(
                new SagaCorrelationProperty("correlation property name", "correlation property value"),
                sagaMetadata, new ContextBag()));

            Assert.AreEqual(id1, id2, "the same input should result in the same id");
        }

        [Test]
        public void Should_generate_different_ids_for_correlation_property_values()
        {
            var generator = new SagaIdGenerator();
            var sagaMetadata = CreateSagaMetadata<SagaTypeA, SagaDataTypeA>();


            var id1 = generator.Generate(new SagaIdGeneratorContext(
                new SagaCorrelationProperty("correlation property name", "A"),
                sagaMetadata, new ContextBag()));
            var id2 = generator.Generate(new SagaIdGeneratorContext(
                new SagaCorrelationProperty("correlation property name", "B"),
                sagaMetadata, new ContextBag()));

            Assert.AreNotEqual(id1, id2, "a different correlation property value should result in a different id");
        }

        [Test]
        public void Should_not_generate_different_ids_for_saga_types()
        {
            var generator = new SagaIdGenerator();

            var id1 = generator.Generate(new SagaIdGeneratorContext(
                new SagaCorrelationProperty("correlation property name", "A"),
                CreateSagaMetadata<SagaTypeA, SagaDataTypeA>(), new ContextBag()));
            var id2 = generator.Generate(new SagaIdGeneratorContext(
                new SagaCorrelationProperty("correlation property name", "A"),
                CreateSagaMetadata<SagaTypeB, SagaDataTypeA>(), new ContextBag()));

            Assert.AreEqual(id1, id2, "a different saga types should not result in a different id");
        }

        [Test]
        public void Should_generate_different_ids_for_saga_data_types()
        {
            var generator = new SagaIdGenerator();

            var id1 = generator.Generate(new SagaIdGeneratorContext(
                new SagaCorrelationProperty("correlation property name", "A"),
                CreateSagaMetadata<SagaTypeA, SagaDataTypeA>(), new ContextBag()));
            var id2 = generator.Generate(new SagaIdGeneratorContext(
                new SagaCorrelationProperty("correlation property name", "A"),
                CreateSagaMetadata<SagaTypeA, SagaDataTypeB>(), new ContextBag()));

            Assert.AreNotEqual(id1, id2, "a different saga data types value should result in a different id");
        }

        [Test]
        public void Should_generate_different_ids_for_correlation_property_name()
        {
            var generator = new SagaIdGenerator();
            var sagaMetadata = CreateSagaMetadata<SagaTypeA, SagaDataTypeA>();

            var id1 = generator.Generate(new SagaIdGeneratorContext(
                new SagaCorrelationProperty("property A", "correlation property value"),
                sagaMetadata, new ContextBag()));
            var id2 = generator.Generate(new SagaIdGeneratorContext(
                new SagaCorrelationProperty("property B", "correlation property value"),
                sagaMetadata, new ContextBag()));

            Assert.AreNotEqual(id1, id2, "a different correlation property should result in a different id");
        }

        static SagaMetadata CreateSagaMetadata<TSagaType, TSagaDataType>()
        {
            var sagaType = typeof(TSagaType);
            var sagaDataType = typeof(TSagaDataType);
            return new SagaMetadata(sagaType.FullName, sagaType, sagaDataType.FullName, sagaDataType,
                null,
                new[]
                {
                    (SagaMessage)Activator.CreateInstance(typeof(SagaMessage),
                        BindingFlags.Instance | BindingFlags.NonPublic, null, new object[] { typeof(string), true },
                        null)
                },
                Array.Empty<SagaFinderDefinition>());
        }

        class SagaTypeA { }
        class SagaTypeB { }
        class SagaDataTypeA { }
        class SagaDataTypeB { }
    }
}