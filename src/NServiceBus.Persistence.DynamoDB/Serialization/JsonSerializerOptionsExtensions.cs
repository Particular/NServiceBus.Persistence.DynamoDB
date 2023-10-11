namespace NServiceBus.Persistence.DynamoDB;

using System;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;

static class JsonSerializerOptionsExtensions
{
    public static JsonSerializerOptions FromWithout<TConverter>(this JsonSerializerOptions options)
        where TConverter : JsonConverter
    {
        var newOptions = new JsonSerializerOptions(options);
        JsonConverter? converterToRemove = null;
        foreach (var converter in newOptions.Converters)
        {
            if (converter is not TConverter)
            {
                continue;
            }

            converterToRemove = converter;
            break;
        }

        if (converterToRemove != null)
        {
            newOptions.Converters.Remove(converterToRemove);
        }
        return newOptions;
    }

    public static bool TryGet<TConverter>(this JsonSerializerOptions options, [NotNullWhen(true)] out IAttributeConverter? converter)
        where TConverter : IAttributeConverter
    {
        ConcurrentDictionary<Type, IAttributeConverter?> cache = attributeConverterCache.GetValue(options, static o => new ConcurrentDictionary<Type, IAttributeConverter?>());
        var cachedConverter = cache.GetOrAdd(typeof(TConverter), static (_, o) =>
        {
            foreach (var converter in o.Converters)
            {
                if (converter is TConverter attributeConverter)
                {
                    return attributeConverter;
                }
            }
            return null;
        }, options);
        converter = cachedConverter;
        return converter is not null;
    }

    static ConditionalWeakTable<JsonSerializerOptions, ConcurrentDictionary<Type, IAttributeConverter?>> attributeConverterCache = [];
}