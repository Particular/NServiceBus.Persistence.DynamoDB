namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Reflection;
    using System.Threading;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.DataModel;
    using Amazon.DynamoDBv2.DocumentModel;
    using Expression = System.Linq.Expressions.Expression;

    static class DynamoDbContextExtensions
    {
        public static Document ConvertToDocument(this DynamoDBContext context, object value, Type type)
        {
            var convert = compileToDocument.Value;
            return convert(context, value, type, cachedConfig);
        }

        public static object ConvertFromDocument(this DynamoDBContext context, Document document, Type type)
        {
            var convert = compileFromDocument.Value;
            return convert(context, document, type, cachedConfig);
        }

        static readonly Lazy<Func<DynamoDBContext, object, Type, DynamoDBOperationConfig, Document>> compileToDocument
            = new(CompileToDocument, LazyThreadSafetyMode.ExecutionAndPublication);

        static readonly Lazy<Func<DynamoDBContext, Document, Type, DynamoDBOperationConfig, object>> compileFromDocument
            = new(CompileFromDocument, LazyThreadSafetyMode.ExecutionAndPublication);

        static readonly DynamoDBOperationConfig cachedConfig = new DynamoDBOperationConfig
        {
            Conversion = DynamoDBEntryConversion.V2
        };

        static Func<DynamoDBContext, object, Type, DynamoDBOperationConfig, Document> CompileToDocument()
        {
            var toDocumentMethod = typeof(DynamoDBContext)
                .GetMethod("SerializeToDocument", BindingFlags.Instance | BindingFlags.NonPublic);
            var flatConfigType =
                typeof(DynamoDBContext).Assembly.GetType("Amazon.DynamoDBv2.DataModel.DynamoDBFlatConfig");
            var constructors = flatConfigType.GetConstructors();
            var contextConfigParameter = Expression.Variable(typeof(DynamoDBContextConfig), "contextConfig");
            var contextParameter = Expression.Parameter(typeof(DynamoDBContext), "context");
            var valueParameter = Expression.Parameter(typeof(object), "value");
            var typeParameter = Expression.Parameter(typeof(Type), "type");
            var configParameter = Expression.Parameter(typeof(DynamoDBOperationConfig), "config");

            var nullAssignment =
                Expression.Assign(contextConfigParameter, Expression.Constant(null, typeof(DynamoDBOperationConfig)));

            var newFlatConfig = Expression.New(constructors[0], configParameter,
                contextConfigParameter);
            var conversionMethodCall =
                Expression.Call(contextParameter, toDocumentMethod, valueParameter, typeParameter, newFlatConfig);
            var body = Expression.Block(new[] { contextConfigParameter }, nullAssignment, conversionMethodCall);
            var toDocument = Expression
                .Lambda<Func<DynamoDBContext, object, Type, DynamoDBOperationConfig, Document>>(
                    body, contextParameter, valueParameter, typeParameter, configParameter)
                .Compile();
            return toDocument;
        }

        static Func<DynamoDBContext, Document, Type, DynamoDBOperationConfig, object> CompileFromDocument()
        {
            var fromDocumentMethod = typeof(DynamoDBContext)
                .GetMethod("DeserializeFromDocument", BindingFlags.Instance | BindingFlags.NonPublic);
            var flatConfigType =
                typeof(DynamoDBContext).Assembly.GetType("Amazon.DynamoDBv2.DataModel.DynamoDBFlatConfig");
            var constructors = flatConfigType.GetConstructors();
            var contextConfigParameter = Expression.Variable(typeof(DynamoDBContextConfig), "contextConfig");
            var contextParameter = Expression.Parameter(typeof(DynamoDBContext), "context");
            var documentParameter = Expression.Parameter(typeof(Document), "document");
            var typeParameter = Expression.Parameter(typeof(Type), "type");
            var configParameter = Expression.Parameter(typeof(DynamoDBOperationConfig), "config");

            var nullAssignment =
                Expression.Assign(contextConfigParameter, Expression.Constant(null, typeof(DynamoDBOperationConfig)));

            var newFlatConfig = Expression.New(constructors[0], configParameter,
                contextConfigParameter);
            var conversionMethodCall =
                Expression.Call(contextParameter, fromDocumentMethod, documentParameter, typeParameter, newFlatConfig);
            var body = Expression.Block(new[] { contextConfigParameter }, nullAssignment, conversionMethodCall);
            var fromDocument = Expression
                .Lambda<Func<DynamoDBContext, Document, Type, DynamoDBOperationConfig, object>>(
                    body, contextParameter, documentParameter, typeParameter, configParameter)
                .Compile();
            return fromDocument;
        }
    }
}