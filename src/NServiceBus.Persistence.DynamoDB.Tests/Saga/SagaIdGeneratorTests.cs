namespace NServiceBus.Persistence.DynamoDB.Tests.Saga;

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
        var sagaMetadata = CreateSagaMetadata();

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
        var sagaMetadata = CreateSagaMetadata();


        var id1 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("correlation property name", "A"),
            sagaMetadata, new ContextBag()));
        var id2 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("correlation property name", "B"),
            sagaMetadata, new ContextBag()));

        Assert.AreNotEqual(id1, id2, "the same input should result in the same id");
    }

    [Test]
    public void Should_generate_different_ids_for_saga_type()
    {
        var generator = new SagaIdGenerator();

        var id1 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("correlation property name", "A"),
            CreateSagaMetadata("sagaA"), new ContextBag()));
        var id2 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("correlation property name", "B"),
            CreateSagaMetadata("sagaB"), new ContextBag()));

        Assert.AreNotEqual(id1, id2, "the same input should result in the same id");
    }

    //TODO: Do we really need to include the correlation property?
    [Test]
    public void Should_ignore_correlation_property_name()
    {
        var generator = new SagaIdGenerator();
        var sagaMetadata = CreateSagaMetadata();

        var id1 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("property A", "correlation property value"),
            sagaMetadata, new ContextBag()));
        var id2 = generator.Generate(new SagaIdGeneratorContext(
            new SagaCorrelationProperty("property B", "correlation property value"),
            sagaMetadata, new ContextBag()));

        Assert.AreEqual(id1, id2, "the same input should result in the same id");
    }

    static SagaMetadata CreateSagaMetadata(string sagaName = null)
    {
        sagaName ??= "MyNamespace.MySaga";
        return new SagaMetadata(sagaName, typeof(object), $"{sagaName}+MySagaData",
            typeof(object),
            new SagaMetadata.CorrelationPropertyMetadata("MyCorrelationProperty", typeof(string)),
            new[]
            {
                (SagaMessage)Activator.CreateInstance(typeof(SagaMessage),
                    BindingFlags.Instance | BindingFlags.NonPublic, null, new object[] { typeof(string), true },
                    null)
            },
            Array.Empty<SagaFinderDefinition>());
    }
}