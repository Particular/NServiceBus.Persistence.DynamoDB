namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using System.Text.Json.Serialization;

    sealed class HashSetOfNumberConverter : JsonConverterFactory
    {
        // This is a cryptic property name to make sure we never clash with the user data
        const string PropertyName = "HashSetNumberContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

        public override bool CanConvert(Type typeToConvert)
        {
            if (!typeToConvert.IsGenericType)
            {
                return false;
            }

            var innerTypeToConvert = typeToConvert.GetGenericArguments()[0];
            var isNumberTypeSupported = innerTypeToConvert == typeof(byte) ||
                                        innerTypeToConvert == typeof(sbyte) ||
                                        innerTypeToConvert == typeof(ushort) ||
                                        innerTypeToConvert == typeof(uint) ||
                                        innerTypeToConvert == typeof(ulong) ||
                                        innerTypeToConvert == typeof(long) ||
                                        innerTypeToConvert == typeof(short) ||
                                        innerTypeToConvert == typeof(int) ||
                                        innerTypeToConvert == typeof(double) ||
                                        innerTypeToConvert == typeof(decimal) ||
                                        innerTypeToConvert == typeof(float);

            return isNumberTypeSupported &&
                   // This check is fairly expensive so we do it only once we know it is a number type that is supported
                   typeToConvert.IsAssignableToGenericType(typeof(ISet<>));
        }

        public override JsonConverter CreateConverter(Type type, JsonSerializerOptions options)
        {
            Type valueType = type.GetGenericArguments()[0];
            var converter = (JsonConverter)Activator.CreateInstance(
                typeof(SetConverter<,>)
                    .MakeGenericType(new Type[] { type, valueType }),
                BindingFlags.Instance | BindingFlags.Public,
                binder: null,
                args: new object[] { options },
                culture: null)!;
            return converter;

        }
        sealed class SetConverter<TSet, TValue> : JsonConverter<TSet>
            where TSet : ISet<TValue>
            where TValue : struct
        {
            public SetConverter(JsonSerializerOptions options)
                => optionsWithoutHashSetOfNumberConverter = options.FromWithout<HashSetOfNumberConverter>();

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

                var set = JsonSerializer.Deserialize<TSet>(ref reader, optionsWithoutHashSetOfNumberConverter);

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
                JsonSerializer.Serialize(writer, value, optionsWithoutHashSetOfNumberConverter);
                writer.WriteEndObject();
            }

            readonly JsonSerializerOptions optionsWithoutHashSetOfNumberConverter;
        }

        public static bool TryExtract(JsonElement element, out List<string?>? numbersAsStrings)
        {
            numbersAsStrings = null;
            if (!element.TryGetProperty(PropertyName, out var property))
            {
                return false;
            }

            numbersAsStrings = new List<string?>(property.GetArrayLength());
            foreach (var innerElement in property.EnumerateArray())
            {
                numbersAsStrings.Add(innerElement.ToString());
            }
            return true;
        }

        public static JsonNode ToNode(List<string> numbersAsStrings)
        {
            var jsonObject = new JsonObject();
            var array = new JsonArray();
            foreach (var numberAsString in numbersAsStrings)
            {
                array.Add(JsonNode.Parse(numberAsString));
            }
            jsonObject.Add(PropertyName, array);
            return jsonObject;
        }
    }
}