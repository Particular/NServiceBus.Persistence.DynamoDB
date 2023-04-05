namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Concurrent;
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

        public static bool Has<TConverter>(this JsonSerializerOptions options)
            where TConverter : JsonConverter
        {
            var cache = hasConverterCache.GetValue(options, static o => new ConcurrentDictionary<Type, bool>());
            return cache.GetOrAdd(typeof(TConverter), static (t, o) =>
            {
                foreach (var converter in o.Converters)
                {
                    if (converter is TConverter)
                    {
                        return true;
                    }
                }
                return false;
            }, options);
        }

        static ConditionalWeakTable<JsonSerializerOptions, ConcurrentDictionary<Type, bool>> hasConverterCache = new();
    }
}