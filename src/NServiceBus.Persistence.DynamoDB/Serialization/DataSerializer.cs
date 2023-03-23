#nullable enable
namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using Amazon.DynamoDBv2.Model;

    static class DataSerializer
    {
        static readonly JsonSerializerOptions serializerOptions =
            new JsonSerializerOptions { Converters = { new MemoryStreamConverter() } };

        public static Dictionary<string, AttributeValue> Serialize<TValue>(TValue value)
        {
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, serializerOptions);
            var attributeMapFromDocument = SerializeToAttributeMap(jsonDocument);
            return attributeMapFromDocument;
        }

        public static TValue? Deserialize<TValue>(Dictionary<string, AttributeValue> attributeValues)
        {
            var jsonObject = DeserializeElementToAttributeMap(attributeValues);
            return jsonObject.Deserialize<TValue>(serializerOptions);
        }

        static Dictionary<string, AttributeValue> SerializeToAttributeMap(JsonDocument document)
        {
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException("TBD");
            }

            return SerializeElementToAttributeMap(document.RootElement);
        }

        static Dictionary<string, AttributeValue> SerializeElementToAttributeMap(JsonElement element)
        {
            var dictionary = new Dictionary<string, AttributeValue>();

            foreach (var property in element.EnumerateObject())
            {
                AttributeValue serializeElement = SerializeElement(property.Value);
                if (serializeElement.NULL)
                {
                    continue;
                }
                dictionary.Add(property.Name, serializeElement);
            }

            return dictionary;
        }

        static AttributeValue SerializeElement(JsonElement element)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                return SerializeElementToMap(element);
            }

            if (element.ValueKind == JsonValueKind.Array)
            {
                return SerializeElementToList(element);
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

            return new AttributeValue(element.GetString());
        }

        static AttributeValue SerializeElementToList(JsonElement element)
        {
            var values = new List<AttributeValue>();
            foreach (var innerElement in element.EnumerateArray())
            {
                AttributeValue serializeElement = SerializeElement(innerElement);
                values.Add(serializeElement);
            }

            // TODO: Can we make this better?
            if (values.All(x => x.B is not null))
            {
                return new AttributeValue { BS = values.Select(x => x.B).ToList() };
            }
            return new AttributeValue { L = values };
        }

        static AttributeValue SerializeElementToMap(JsonElement element)
        {
            foreach (var property in element.EnumerateObject())
            {
                if (property.NameEquals(MemoryStreamConverter.PropertyName))
                {
                    return new AttributeValue { B = new MemoryStream(property.Value.GetBytesFromBase64()) };
                }
            }
            return new AttributeValue { M = SerializeElementToAttributeMap(element) };
        }

        static JsonObject DeserializeElementToAttributeMap(Dictionary<string, AttributeValue> attributeValues)
        {
            var jsonObject = new JsonObject();
            foreach (var kvp in attributeValues)
            {
                AttributeValue attributeValue = kvp.Value;
                string attributeName = kvp.Key;
                if (attributeValue.IsMSet)
                {
                    jsonObject.Add(attributeName, DeserializeElementToAttributeMap(attributeValue.M));
                    continue;
                }

                if (attributeValue.B != null)
                {
                    jsonObject.Add(attributeName, new JsonObject
                    {
                        [MemoryStreamConverter.PropertyName] = Convert.ToBase64String(attributeValue.B.ToArray())
                    });
                    continue;
                }

                if (attributeValue.BS is { Count: > 0 })
                {
                    var array = new JsonArray();
                    foreach (var memoryStream in attributeValue.BS)
                    {
                        array.Add(new JsonObject
                        {
                            [MemoryStreamConverter.PropertyName] = Convert.ToBase64String(memoryStream.ToArray())
                        });
                    }
                    jsonObject.Add(attributeName, array);
                    continue;
                }

                if (attributeValue.IsBOOLSet)
                {
                    jsonObject.Add(attributeName, attributeValue.BOOL);
                    continue;
                }

                if (attributeValue.NULL)
                {
                    jsonObject.Add(attributeName, default);
                    continue;
                }

                if (attributeValue.N != null)
                {
                    jsonObject.Add(attributeName, attributeValue.N);
                    continue;
                }

                jsonObject.Add(attributeName, attributeValue.S);

            }

            return jsonObject;
        }
    }
}