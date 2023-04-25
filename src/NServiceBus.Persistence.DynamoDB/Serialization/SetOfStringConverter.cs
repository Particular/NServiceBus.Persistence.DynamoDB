namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Reflection;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using System.Text.Json.Serialization;
    using Amazon.DynamoDBv2.Model;

    sealed class SetOfStringConverter : JsonConverterFactory, IAttributeConverter
    {
        // This is a cryptic property name to make sure we never clash with the user data
        const string PropertyName = "HashSetStringContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

        public override bool CanConvert(Type typeToConvert)
            => typeToConvert.IsGenericType && typeof(ISet<string>).IsAssignableFrom(typeToConvert);

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            var converter = (JsonConverter)Activator.CreateInstance(
                typeof(SetConverter<>)
                    .MakeGenericType(new Type[] { typeToConvert }),
                BindingFlags.Instance | BindingFlags.Public,
                binder: null,
                args: new object[] { options },
                culture: null)!;
            return converter;
        }

        public bool TryExtract(JsonElement element, [NotNullWhen(true)] out AttributeValue? attributeValue)
        {
            attributeValue = null;
            if (!element.TryGetProperty(PropertyName, out var property))
            {
                return false;
            }

            var strings = new List<string?>(property.GetArrayLength());
            foreach (var innerElement in property.EnumerateArray())
            {
                strings.Add(innerElement.GetString());
            }
            attributeValue = new AttributeValue { SS = strings };
            return true;
        }

        public JsonNode ToNode(AttributeValue attributeValue)
        {
            var jsonObject = new JsonObject();
            var array = new JsonArray();
            foreach (var value in attributeValue.SS)
            {
                array.Add(value);
            }
            jsonObject.Add(PropertyName, array);
            return jsonObject;
        }

        sealed class SetConverter<TSet> : JsonConverter<TSet> where TSet : ISet<string>
        {
            public SetConverter(JsonSerializerOptions options)
                => optionsWithoutSetOfStringConverter = options.FromWithout<SetOfStringConverter>();

            public override TSet? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType != JsonTokenType.StartObject)
                {
                    throw new JsonException();
                }

                reader.Read();
                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    throw new JsonException();
                }

                string? propertyName = reader.GetString();
                if (propertyName != PropertyName)
                {
                    throw new JsonException();
                }

                reader.Read();
                if (reader.TokenType != JsonTokenType.StartArray)
                {
                    throw new JsonException();
                }

                var set = JsonSerializer.Deserialize<TSet>(ref reader, optionsWithoutSetOfStringConverter);

                reader.Read();

                if (reader.TokenType != JsonTokenType.EndObject)
                {
                    throw new JsonException();
                }
                return set;
            }

            public override void Write(Utf8JsonWriter writer, TSet value, JsonSerializerOptions options)
            {
                writer.WriteStartObject();
                writer.WritePropertyName(PropertyName);
                JsonSerializer.Serialize(writer, value, optionsWithoutSetOfStringConverter);
                writer.WriteEndObject();
            }

            readonly JsonSerializerOptions optionsWithoutSetOfStringConverter;
        }
    }
}