﻿namespace NServiceBus.Persistence.DynamoDB.Tests;

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Extensibility;
using NUnit.Framework;
using Particular.Approvals;
using Sagas;
using Testing;

[TestFixture]
public class SagaSchemaVersionTests
{
    [Test]
    public async Task Should_update_schema_version_on_schema_changes()
    {
        var sagaPersister = new SagaPersister(new MockDynamoDBClient(), new SagaPersistenceConfiguration(), "SchemaVersionTest");

        var sagaData = new TestSagaData()
        {
            CorrelationProperty = "CorrelationPropertyValue",
            Id = new Guid("FFC8A2FD-0335-47C8-A29D-9EEA6C8445D8"),
            OriginalMessageId = "OriginalMessageIdValue",
            Originator = "OriginatorValue"
        };

        var testableSession = new TestableDynamoSynchronizedStorageSession();
        await sagaPersister.Save(sagaData,
            new SagaCorrelationProperty(nameof(TestSagaData.CorrelationProperty), sagaData.CorrelationProperty),
            testableSession, new ContextBag());

        // !!! IMPORTANT !!!
        // This test should help to detect data schema changes.
        // When this test fails, make sure to update the saga data schema version in the metadata.
        Approver.Verify(
            JsonSerializer.Serialize(testableSession.TransactWriteItems.Single().Put.Item,
                new JsonSerializerOptions { WriteIndented = true }));
    }

    [Theory]
    [TestCase("SchemaVersionTest")]
    [TestCase("schemaversiontest")]
    [TestCase("SCHEMAVERSIONTEST")]
    public async Task Should_treat_identifier_case_sensitive(string endpointIdentifier)
    {
        var sagaPersister = new SagaPersister(new MockDynamoDBClient(), new SagaPersistenceConfiguration(), endpointIdentifier);

        var sagaData = new TestSagaData()
        {
            CorrelationProperty = "CorrelationPropertyValue",
            Id = new Guid("b36125bf-f783-40c2-9eac-4c7433afa4fa"),
            OriginalMessageId = "OriginalMessageIdValue",
            Originator = "OriginatorValue"
        };

        var testableSession = new TestableDynamoSynchronizedStorageSession();
        await sagaPersister.Save(sagaData,
            new SagaCorrelationProperty(nameof(TestSagaData.CorrelationProperty), sagaData.CorrelationProperty),
            testableSession, new ContextBag());

        Assert.That(testableSession.TransactWriteItems.Single().Put.Item["PK"].S, Does.Contain(endpointIdentifier));
    }

    class TestSagaData : ContainSagaData
    {
        public string CorrelationProperty { get; set; }
    }
}