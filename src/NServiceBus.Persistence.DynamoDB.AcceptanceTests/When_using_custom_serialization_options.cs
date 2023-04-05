namespace NServiceBus.AcceptanceTests
{
    using System;
    using System.IO;
    using System.Text;
    using System.Text.Json;
    using System.Text.Json.Serialization;
    using System.Threading.Tasks;
    using AcceptanceTesting;
    using EndpointTemplates;
    using NUnit.Framework;
    using Persistence.DynamoDB;

    public class When_using_custom_serialization_options : NServiceBusAcceptanceTest
    {
        [Test]
        public async Task Should_use_them()
        {
            var dataId = Guid.NewGuid();

            var context = await Scenario.Define<Context>()
                .WithEndpoint<EndpointThatHostsASaga>(
                    b => b.When(session => session.SendLocal(new StartSaga { DataId = dataId })))
                .Done(c => c.SagaDone)
                .Run();

            Assert.True(context.SagaDone);
            Assert.That(context.CustomSerializedProperty, Is.EqualTo(dataId.ToString()));
            Assert.That(context.StreamContent, Is.EqualTo(dataId.ToString()));
        }

        public class Context : ScenarioContext
        {
            public bool SagaDone { get; set; }
            public string CustomSerializedProperty { get; set; }
            public string StreamContent { get; set; }
        }

        public class EndpointThatHostsASaga : EndpointConfigurationBuilder
        {
            public EndpointThatHostsASaga() =>
                EndpointSetup<DefaultServer>(c =>
                {
                    var persistence = c.UsePersistence<DynamoDBPersistence>();
                    var sagas = persistence.Sagas();
                    sagas.MapperOptions = new JsonSerializerOptions(sagas.MapperOptions)
                    {
                        TypeInfoResolver = new SagaJsonContext(sagas.MapperOptions),
                        Converters = { new CustomConverter() }
                    };
                });

            class CustomConverter : JsonConverter<MySagaData>
            {
                public override MySagaData Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
                {
                    var sagaData = JsonSerializer.Deserialize<MySagaData>(ref reader, options.FromWithout<CustomConverter>());
                    sagaData.ManagedByConverter = sagaData.Nested.SomeProperty;
                    return sagaData;
                }

                public override void Write(Utf8JsonWriter writer, MySagaData value, JsonSerializerOptions options)
                {
                    value.ManagedByConverter = value.Nested.SomeProperty;
                    JsonSerializer.Serialize(writer, value, options.FromWithout<CustomConverter>());
                }
            }

            public class MySaga : Saga<MySagaData>,
                IAmStartedByMessages<StartSaga>,
                IHandleMessages<ContinueSaga>
            {
                public MySaga(Context context) => testContext = context;

                public Task Handle(StartSaga message, IMessageHandlerContext context)
                {
                    Data.DataId = message.DataId;
                    Data.Nested = new NestedObject { SomeProperty = message.DataId.ToString() };
                    Data.SomeStream = new MemoryStream(Encoding.UTF8.GetBytes(message.DataId.ToString()));

                    return context.SendLocal(new ContinueSaga { DataId = message.DataId });
                }

                public Task Handle(ContinueSaga message, IMessageHandlerContext context)
                {
                    MarkAsComplete();

                    testContext.CustomSerializedProperty = Data.ManagedByConverter;
                    testContext.StreamContent = Encoding.UTF8.GetString(Data.SomeStream.ToArray());
                    testContext.SagaDone = true;
                    return Task.CompletedTask;
                }

                protected override void ConfigureHowToFindSaga(SagaPropertyMapper<MySagaData> mapper) =>
                    mapper.MapSaga(m => m.DataId)
                        .ToMessage<StartSaga>(m => m.DataId)
                        .ToMessage<ContinueSaga>(m => m.DataId);

                readonly Context testContext;
            }
        }

        public class StartSaga : IMessage
        {
            public Guid DataId { get; set; }
        }

        public class ContinueSaga : IMessage
        {
            public Guid DataId { get; set; }
        }
    }

    public class MySagaData : ContainSagaData
    {
        public virtual Guid DataId { get; set; }

        public virtual NestedObject Nested { get; set; }

        public string ManagedByConverter { get; set; }

        // here to demonstrate the custom converters still work
        public MemoryStream SomeStream { get; set; }
    }

    public class NestedObject
    {
        public string SomeProperty { get; set; }
    }

    [JsonSourceGenerationOptions(WriteIndented = true)]
    [JsonSerializable(typeof(MySagaData))]
    partial class SagaJsonContext : JsonSerializerContext
    {
    }
}