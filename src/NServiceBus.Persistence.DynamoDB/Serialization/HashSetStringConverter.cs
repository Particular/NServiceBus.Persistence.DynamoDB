#nullable enable
namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Text.Json;
    using System.Text.Json.Serialization;

    sealed class HashSetStringConverter : JsonConverterFactory
    {
        public const string PropertyName = "HashSetStringContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

        public override bool CanConvert(Type typeToConvert)
            => typeToConvert.IsGenericType && typeof(ISet<string>).IsAssignableFrom(typeToConvert);

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            var converter = (JsonConverter)Activator.CreateInstance(
                typeof(SetConverter<>)
                    .MakeGenericType(new Type[] { typeToConvert }),
                BindingFlags.Instance | BindingFlags.Public,
                binder: null,
                args: null,
                culture: null)!;
            return converter;
        }

        sealed class SetConverter<TSet> : JsonConverter<TSet> where TSet : ISet<string>
        {
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

                // Deliberately not passing the options to use the default json serialization behavior
                var set = JsonSerializer.Deserialize<TSet>(ref reader);

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
                writer.WriteStartArray();
                foreach (string s in value)
                {
                    writer.WriteStringValue(s);
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
            }
        }
    }
}