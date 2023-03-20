namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using Amazon.DynamoDBv2.Model;

    static class DataSerializer
    {
        static readonly JsonSerializerOptions serializerOptions =
            new JsonSerializerOptions { Converters = { new MemoryStreamConverter() } };

        public static Dictionary<string, AttributeValue> Serialize<TValue>(TValue value)
        {
            var jsonDocument = JsonSerializer.SerializeToDocument(value, serializerOptions);
            var attributeMapFromDocument = SerializeElementToAttributeMap(jsonDocument.RootElement);
            return attributeMapFromDocument;
        }

        public static TValue Deserialize<TValue>(Dictionary<string, AttributeValue> attributeValues)
        {
            var jsonObject = DeserializeElementToAttributeMap(attributeValues);
            return jsonObject.Deserialize<TValue>(serializerOptions);
        }

        static Dictionary<string, AttributeValue> SerializeElementToAttributeMap(JsonElement element)
        {
            var dictionary = new Dictionary<string, AttributeValue>();
            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in element.EnumerateObject())
                {
                    AttributeValue serializeElement;
                    if (property.NameEquals(MemoryStreamConverter.PropertyName))
                    {
                        serializeElement = new AttributeValue { B = new MemoryStream(property.Value.GetBytesFromBase64()) };
                    }
                    else
                    {
                        serializeElement = SerializeElement(property.Value);
                    }

                    if (serializeElement.NULL)
                    {
                        continue;
                    }
                    dictionary.Add(property.Name, serializeElement);
                }
            }

            return dictionary;
        }

        static AttributeValue SerializeElement(JsonElement element)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                return new AttributeValue { M = SerializeElementToAttributeMap(element) };
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

            return new AttributeValue(element.GetString());
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