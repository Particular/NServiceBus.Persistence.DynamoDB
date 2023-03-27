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

            if (typeToConvert.GetGenericTypeDefinition() != typeof(HashSet<>))
            {
                return false;
            }

            return IsNumericType(typeToConvert.GetGenericArguments()[0]);
        }

        public override JsonConverter CreateConverter(
            Type type,
            JsonSerializerOptions options)
        {
            Type valueType = type.GetGenericArguments()[0];

            var converter = (JsonConverter)Activator.CreateInstance(
                typeof(HashSetValueConverter<>).MakeGenericType(
                    new Type[] { valueType }),
                BindingFlags.Instance | BindingFlags.Public,
                binder: null,
                args: new object[] { options },
                culture: null)!;

            return converter;
        }

        static bool IsNumericType(Type type) =>
            Type.GetTypeCode(type) is TypeCode.Byte or TypeCode.SByte or TypeCode.UInt16 or TypeCode.UInt32
                or TypeCode.UInt64
                or TypeCode.Int16 or TypeCode.Int32 or TypeCode.Int64 or TypeCode.Decimal or TypeCode.Double
                or TypeCode.Single;


        sealed class HashSetValueConverter<TValue> :
            JsonConverter<HashSet<TValue>> where TValue : struct
        {
            public HashSetValueConverter(JsonSerializerOptions options)
            {
                // For performance, use the existing converter.
                valueConverter = (JsonConverter<TValue>)options
                    .GetConverter(typeof(TValue));

                // Cache the key and value types.
                valueType = typeof(TValue);
            }

            public override HashSet<TValue>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
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

                reader.Read();

                var hashset = new HashSet<TValue>();
                while (reader.TokenType != JsonTokenType.EndArray)
                {
                    hashset.Add(valueConverter.Read(ref reader, valueType, options));

                    reader.Read();
                }

                reader.Read();

                if (reader.TokenType != JsonTokenType.EndObject)
                {
                    throw new JsonException();
                }
                return hashset;
            }

            public override void Write(Utf8JsonWriter writer, HashSet<TValue> value, JsonSerializerOptions options)
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
            readonly Type valueType;
        }
    }
}