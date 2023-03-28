#nullable enable
namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Text.Json;
    using System.Text.Json.Serialization;

    sealed class HashSetOfNumberConverter : JsonConverterFactory
    {
        public const string PropertyName = "HashSetNumberContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

        public override bool CanConvert(Type typeToConvert)
        {
            if (!typeToConvert.IsGenericType)
            {
                return false;
            }

            if (!typeToConvert.IsAssignableToGenericType(typeof(ISet<>)))
            {
                return false;
            }

            var innerTypeToConvert = typeToConvert.GetGenericArguments()[0];
            return innerTypeToConvert == typeof(byte) ||
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
        }

        public override JsonConverter CreateConverter(Type type, JsonSerializerOptions options)
        {
            Type valueType = type.GetGenericArguments()[0];
            var converter = (JsonConverter)Activator.CreateInstance(
                typeof(SetConverter<,>)
                    .MakeGenericType(new Type[] { type, valueType }),
                BindingFlags.Instance | BindingFlags.Public,
                binder: null,
                args: new[] { options },
                culture: null)!;
            return converter;

        }
        sealed class SetConverter<TSet, TValue> : JsonConverter<TSet>
            where TSet : ISet<TValue>
            where TValue : struct
        {
            public SetConverter(JsonSerializerOptions options) =>
                // For performance, use the existing converter.
                valueConverter = (JsonConverter<TValue>)options
                    .GetConverter(typeof(TValue));

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
                foreach (TValue s in value)
                {
                    valueConverter.Write(writer, s, options);
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
            }

            readonly JsonConverter<TValue> valueConverter;
        }
    }
}