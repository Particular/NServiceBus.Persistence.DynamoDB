namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.DocumentModel;
    using Amazon.DynamoDBv2.Model;
    using Expression = System.Linq.Expressions.Expression;

    static class Mapping
    {
        static Mapping()
        {
            var v2Conversion = typeof(DynamoDBEntryConversion).GetProperty("V2", BindingFlags.Public | BindingFlags.Static);
            var v2ConversionValue = v2Conversion.GetValue(null);
            var conversionFields = typeof(DynamoDBEntryConversion).GetFields(BindingFlags.Instance | BindingFlags.NonPublic);
            var converterCacheField = conversionFields
                .Single(f => f.FieldType.FullName == "Amazon.DynamoDBv2.ConverterCache");
            var fieldValue = converterCacheField.GetValue(v2ConversionValue);
            var dictionaryField = fieldValue.GetType().GetFields(BindingFlags.Instance | BindingFlags.NonPublic)
                .Single(f => f.Name == "Cache");
            object value = dictionaryField.FieldType.GetProperty("Keys").GetValue(dictionaryField.GetValue(fieldValue));
            var supportedTypesKeys = (IEnumerable<Type>)value;
            supportedTypes = supportedTypesKeys.ToHashSet();

            Debug.Assert(fieldValue != null);
        }

        public static Dictionary<string, AttributeValue> ToAttributes(IContainSagaData sagaData)
        {
            var accessors = GetPropertyAccessors(sagaData.GetType());
            var document = new Document();
            foreach (var accessor in accessors)
            {
                var name = accessor.Name;
                var value = accessor.Getter(sagaData);

                if (supportedTypes.Contains(accessor.PropertyType))
                {
                    var method = DynamoDBEntryConversion.V2.GetType().GetMethods()
                        .Single(m => m.Name == "TryConvertToEntry" && m.GetParameters().Any(p => p.ParameterType == typeof(Type)));
                    var parameters = new object[] { accessor.PropertyType, value, default, };
                    var result = (bool)method.Invoke(DynamoDBEntryConversion.V2, parameters);
                    if (result)
                    {
                        document.Add(name, (DynamoDBEntry)parameters[2]);
                    }
                }
            }
            return document.ToAttributeMap();
        }

        static IReadOnlyCollection<PropertyAccessor> GetPropertyAccessors(Type sagaDataType)
        {
            var accessors = propertyAccessorCache.GetOrAdd(sagaDataType, static dataType =>
            {
                var entityProperties = dataType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
                var setters = new PropertyAccessor[entityProperties.Length];
                int index = 0;
                foreach (var propertyInfo in entityProperties)
                {
                    setters[index] = new PropertyAccessor(propertyInfo);
                    index++;
                }
                return setters;
            });
            return accessors;
        }

        static readonly ConcurrentDictionary<Type, PropertyAccessor[]> propertyAccessorCache = new();
        static readonly HashSet<Type> supportedTypes;

        sealed class PropertyAccessor
        {
            public PropertyAccessor(PropertyInfo propertyInfo)
            {
                Setter = GenerateSetter(propertyInfo);
                Getter = GenerateGetter(propertyInfo);
                Name = propertyInfo.Name;
                PropertyType = propertyInfo.PropertyType;
            }

            static Func<object, object> GenerateGetter(PropertyInfo propertyInfo)
            {
                var instance = Expression.Parameter(typeof(object), "instance");
                var instanceCast = !propertyInfo.DeclaringType.IsValueType
                    ? Expression.TypeAs(instance, propertyInfo.DeclaringType)
                    : Expression.Convert(instance, propertyInfo.DeclaringType);
                var getter = Expression
                    .Lambda<Func<object, object>>(
                        Expression.TypeAs(Expression.Call(instanceCast, propertyInfo.GetGetMethod()), typeof(object)), instance)
                    .Compile();
                return getter;
            }

            static Action<object, object> GenerateSetter(PropertyInfo propertyInfo)
            {
                var instance = Expression.Parameter(typeof(object), "instance");
                var value = Expression.Parameter(typeof(object), "value");
                // value as T is slightly faster than (T)value, so if it's not a value type, use that
                var instanceCast = !propertyInfo.DeclaringType.IsValueType
                    ? Expression.TypeAs(instance, propertyInfo.DeclaringType)
                    : Expression.Convert(instance, propertyInfo.DeclaringType);
                var valueCast = !propertyInfo.PropertyType.IsValueType
                    ? Expression.TypeAs(value, propertyInfo.PropertyType)
                    : Expression.Convert(value, propertyInfo.PropertyType);
                var setter = Expression
                    .Lambda<Action<object, object>>(Expression.Call(instanceCast, propertyInfo.GetSetMethod(), valueCast), instance,
                        value).Compile();
                return setter;
            }

            public Action<object, object> Setter { get; }
            public Func<object, object> Getter { get; }
            public string Name { get; }
            public Type PropertyType { get; }
        }
    }
}