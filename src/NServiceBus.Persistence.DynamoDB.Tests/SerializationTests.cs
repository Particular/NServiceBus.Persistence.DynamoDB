using System.Drawing.Imaging;
using System.IO;
using System.Text;
using System.Text.Json.Serialization;

namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Text.Json;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.DataModel;
    using Amazon.DynamoDBv2.Model;
    using Amazon.Runtime;
    using NServiceBus.Persistence.DynamoDB;
    using NUnit.Framework;
    using Particular.Approvals;

    public class SerializationTests
    {
        ComplexSagaData sagaData;
        JsonSerializerOptions serializerOptions;
        DynamoDBContext context;

        public class ComplexSagaData : ContainSagaData
        {
            public Guid SomeId { get; set; }
            public int SomeInt { get; set; }
            public int? NullableInt { get; set; }

            public int[] IntArray { get; set; }
            public double? NullableDouble { get; set; }

            public bool? NullableBool { get; set; }
            public Guid? NullableGuid { get; set; }
            public long? NullableLong { get; set; }
            public byte[] ByteArray { get; set; }

            public DateTime DateTime { get; set; }

            //public DateTimeOffset DateTimeOffset { get; set; }

            public List<NestedData> NestedData { get; set; }

            public Dictionary<string, string> Dictionary { get; set; }

            public NestedData SomeMoreNestedData { get; set; }

            public MemoryStream SomeStream { get; set; }
        }

        public class NestedData
        {
            public int[] IntArray { get; set; }
        }

        [SetUp]
        public void SetUp()
        {
            context = new DynamoDBContext(new AmazonDynamoDBClient(new EnvironmentVariablesAWSCredentials()));

            sagaData = new ComplexSagaData
            {
                Id = new Guid("9A2944FB-F0F3-432F-80B0-D2C6F42C76B0"),
                OriginalMessageId = "746AB276-9B28-49C9-BF92-AF749FBE2ADE",
                Originator = "Originator",
                SomeId = new Guid("E35B9F11-E8EC-497F-A923-9D280B483630"),
                SomeInt = 42,
                IntArray = new[] { 42, 43 },
                DateTime = new DateTime(2023, 11, 12, 1, 1, 1, 1, DateTimeKind.Utc),
                //DateTimeOffset = new DateTimeOffset(2023, 11, 12, 1, 1, 1, 1, TimeSpan.FromHours(1)),
                NestedData = new List<NestedData>
                {
                    new NestedData
                    {
                        IntArray = new[] { 42, 43 },
                    }
                },
                Dictionary = new Dictionary<string, string>
                {
                    { "Foo", "Bar" }
                },
                SomeMoreNestedData = new NestedData
                {
                    IntArray = new[] { 42, 43 },
                },
                SomeStream = new MemoryStream(Encoding.UTF8.GetBytes("Hello World"))
            };

            serializerOptions = new JsonSerializerOptions
            {
                Converters = { new MemoryStreamConverter() }
            };
        }
        
        public class MemoryStreamConverter : JsonConverter<MemoryStream>
        {
            public override MemoryStream Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                return new MemoryStream(reader.GetBytesFromBase64());
            }

            public override void Write(Utf8JsonWriter writer, MemoryStream value, JsonSerializerOptions options)
            {
                writer.WriteBase64StringValue(value.ToArray());
            }
        }

        [Test]
        public void ToJsonDocument()
        {
            var jsonDocument = JsonSerializer.SerializeToDocument(sagaData, sagaData.GetType(), serializerOptions);

            var attributeMapFromDocument = Serialize(jsonDocument.RootElement);

            Assert.NotNull(jsonDocument);
            // Approver.Verify(attributeMapFromDocument, scenario: "ManualMapping");

            var document = context.ConvertToDocument(sagaData, sagaData.GetType());
            var attributeMap = document.ToAttributeMap();

            Approver.Verify(attributeMap, scenario: "SDKMapping");
        }

        Dictionary<string, AttributeValue> Serialize(JsonElement element)
        {
            var dictionary = new Dictionary<string, AttributeValue>();
            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in element.EnumerateObject())
                {
                    AttributeValue serializeElement = SerializeElement(property.Value);
                    if (serializeElement.NULL)
                    {
                        continue;
                    }
                    dictionary.Add(property.Name, serializeElement);
                }
            }

            return dictionary;
        }

        AttributeValue SerializeElement(JsonElement element)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                return new AttributeValue { M = Serialize(element) };
            }

            if (element.ValueKind == JsonValueKind.Array)
            {
                var values = new List<AttributeValue>();
                foreach (var innerElement in element.EnumerateArray())
                {
                    values.Add(SerializeElement(innerElement));
                }
                return new AttributeValue { L = values };
            }

            if (element.ValueKind == JsonValueKind.False)
            {
                return new AttributeValue { BOOL = false };
            }

            if (element.ValueKind == JsonValueKind.True)
            {
                return new AttributeValue { BOOL = true };
            }

            if (element.ValueKind == JsonValueKind.Null)
            {
                return new AttributeValue { NULL = true };
            }

            if (element.ValueKind == JsonValueKind.Number)
            {
                return new AttributeValue { N = element.ToString() };
            }

            if (element.TryGetBytesFromBase64(out var buffer))
            {
                return new AttributeValue { B = new MemoryStream(buffer) };
            }
            return new AttributeValue(element.GetString());
        }
    }
}

namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Reflection;
    using System.Threading;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.DataModel;
    using Amazon.DynamoDBv2.DocumentModel;
    using Expression = System.Linq.Expressions.Expression;

    static class DynamoDbContextExtensions
    {
        public static Document ConvertToDocument(this DynamoDBContext context, object value, Type type)
        {
            var convert = compileToDocument.Value;
            return convert(context, value, type, cachedConfig);
        }

        public static object ConvertFromDocument(this DynamoDBContext context, Document document, Type type)
        {
            var convert = compileFromDocument.Value;
            return convert(context, document, type, cachedConfig);
        }

        static readonly Lazy<Func<DynamoDBContext, object, Type, DynamoDBOperationConfig, Document>> compileToDocument
            = new(CompileToDocument, LazyThreadSafetyMode.ExecutionAndPublication);

        static readonly Lazy<Func<DynamoDBContext, Document, Type, DynamoDBOperationConfig, object>> compileFromDocument
            = new(CompileFromDocument, LazyThreadSafetyMode.ExecutionAndPublication);

        static readonly DynamoDBOperationConfig cachedConfig = new DynamoDBOperationConfig
        {
            Conversion = DynamoDBEntryConversion.V2
        };

        static Func<DynamoDBContext, object, Type, DynamoDBOperationConfig, Document> CompileToDocument()
        {
            var toDocumentMethod = typeof(DynamoDBContext)
                .GetMethod("SerializeToDocument", BindingFlags.Instance | BindingFlags.NonPublic);
            var flatConfigType =
                typeof(DynamoDBContext).Assembly.GetType("Amazon.DynamoDBv2.DataModel.DynamoDBFlatConfig");
            var constructors = flatConfigType.GetConstructors();
            var contextConfigParameter = Expression.Variable(typeof(DynamoDBContextConfig), "contextConfig");
            var contextParameter = Expression.Parameter(typeof(DynamoDBContext), "context");
            var valueParameter = Expression.Parameter(typeof(object), "value");
            var typeParameter = Expression.Parameter(typeof(Type), "type");
            var configParameter = Expression.Parameter(typeof(DynamoDBOperationConfig), "config");

            var nullAssignment =
                Expression.Assign(contextConfigParameter, Expression.Constant(null, typeof(DynamoDBOperationConfig)));

            var newFlatConfig = Expression.New(constructors[0], configParameter,
                contextConfigParameter);
            var conversionMethodCall =
                Expression.Call(contextParameter, toDocumentMethod, valueParameter, typeParameter, newFlatConfig);
            var body = Expression.Block(new[] { contextConfigParameter }, nullAssignment, conversionMethodCall);
            var toDocument = Expression
                .Lambda<Func<DynamoDBContext, object, Type, DynamoDBOperationConfig, Document>>(
                    body, contextParameter, valueParameter, typeParameter, configParameter)
                .Compile();
            return toDocument;
        }

        static Func<DynamoDBContext, Document, Type, DynamoDBOperationConfig, object> CompileFromDocument()
        {
            var fromDocumentMethod = typeof(DynamoDBContext)
                .GetMethod("DeserializeFromDocument", BindingFlags.Instance | BindingFlags.NonPublic);
            var flatConfigType =
                typeof(DynamoDBContext).Assembly.GetType("Amazon.DynamoDBv2.DataModel.DynamoDBFlatConfig");
            var constructors = flatConfigType.GetConstructors();
            var contextConfigParameter = Expression.Variable(typeof(DynamoDBContextConfig), "contextConfig");
            var contextParameter = Expression.Parameter(typeof(DynamoDBContext), "context");
            var documentParameter = Expression.Parameter(typeof(Document), "document");
            var typeParameter = Expression.Parameter(typeof(Type), "type");
            var configParameter = Expression.Parameter(typeof(DynamoDBOperationConfig), "config");

            var nullAssignment =
                Expression.Assign(contextConfigParameter, Expression.Constant(null, typeof(DynamoDBOperationConfig)));

            var newFlatConfig = Expression.New(constructors[0], configParameter,
                contextConfigParameter);
            var conversionMethodCall =
                Expression.Call(contextParameter, fromDocumentMethod, documentParameter, typeParameter, newFlatConfig);
            var body = Expression.Block(new[] { contextConfigParameter }, nullAssignment, conversionMethodCall);
            var fromDocument = Expression
                .Lambda<Func<DynamoDBContext, Document, Type, DynamoDBOperationConfig, object>>(
                    body, contextParameter, documentParameter, typeParameter, configParameter)
                .Compile();
            return fromDocument;
        }
    }
}