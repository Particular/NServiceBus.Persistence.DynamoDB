namespace NServiceBus.Persistence.DynamoDB.Tests;

using System;
using System.Threading.Tasks;
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
        var sagaMetadata = SagaMetadata.Create<SagaTypeA>();

        var id1 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("correlation property name", "correlation property value"),
            sagaMetadata, new ContextBag()));
        var id2 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("correlation property name", "correlation property value"),
            sagaMetadata, new ContextBag()));

        Assert.That(id2, Is.EqualTo(id1), "the same input should result in the same id");
    }

    [Test]
    public void Should_generate_different_ids_for_correlation_property_values()
    {
        var generator = new SagaIdGenerator();
        var sagaMetadata = SagaMetadata.Create<SagaTypeA>();


        var id1 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("correlation property name", "A"),
            sagaMetadata, new ContextBag()));
        var id2 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("correlation property name", "B"),
            sagaMetadata, new ContextBag()));

        Assert.That(id2, Is.Not.EqualTo(id1), "a different correlation property value should result in a different id");
    }

    [Test]
    public void Should_not_generate_different_ids_for_saga_types()
    {
        var generator = new SagaIdGenerator();

        var id1 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("correlation property name", "A"),
            SagaMetadata.Create<SagaTypeA>(), new ContextBag()));
        var id2 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("correlation property name", "A"),
            SagaMetadata.Create<SagaTypeA>(), new ContextBag()));

        Assert.That(id2, Is.EqualTo(id1), "a different saga types should not result in a different id");
    }

    [Test]
    public void Should_generate_different_ids_for_saga_data_types()
    {
        var generator = new SagaIdGenerator();

        var id1 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("correlation property name", "A"), SagaMetadata.Create<SagaTypeA>(), new ContextBag()));
        var id2 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("correlation property name", "A"), SagaMetadata.Create<SagaTypeB>(), new ContextBag()));

        Assert.That(id2, Is.Not.EqualTo(id1), "a different saga data types value should result in a different id");
    }

    [Test]
    public void Should_generate_different_ids_for_correlation_property_name()
    {
        var generator = new SagaIdGenerator();
        var sagaMetadata = SagaMetadata.Create<SagaTypeA>();

        var id1 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("property A", "correlation property value"),
            sagaMetadata, new ContextBag()));
        var id2 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("property B", "correlation property value"),
            sagaMetadata, new ContextBag()));

        Assert.That(id2, Is.Not.EqualTo(id1), "a different correlation property should result in a different id");
    }

    class SagaTypeA : Saga<SagaDataTypeA>, IAmStartedByMessages<MyMessage>
    {
        protected override void ConfigureHowToFindSaga(SagaPropertyMapper<SagaDataTypeA> mapper) => mapper.MapSaga(s => s.CorrelationId)
            .ToMessage<MyMessage>(m => m.CorrelationId);

        public Task Handle(MyMessage message, IMessageHandlerContext context) => throw new NotImplementedException();
    }

    class SagaTypeB : Saga<SagaDataTypeB>, IAmStartedByMessages<MyMessage>
    {
        protected override void ConfigureHowToFindSaga(SagaPropertyMapper<SagaDataTypeB> mapper) => mapper.MapSaga(s => s.CorrelationId)
            .ToMessage<MyMessage>(m => m.CorrelationId);

        public Task Handle(MyMessage message, IMessageHandlerContext context) => throw new NotImplementedException();
    }

    class SagaDataTypeA : ContainSagaData
    {
        public string CorrelationId { get; set; }
    }

    class SagaDataTypeB : ContainSagaData
    {
        public string CorrelationId { get; set; }
    }


    class MyMessage
    {
        public string CorrelationId { get; set; }
    }
}