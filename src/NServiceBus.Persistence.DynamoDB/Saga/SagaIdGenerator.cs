namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using Sagas;

    class SagaIdGenerator : ISagaIdGenerator
    {
        public Guid Generate(SagaIdGeneratorContext context)
        {
            if (context.CorrelationProperty == SagaCorrelationProperty.None)
            {
                throw new Exception("The DynamoDB saga persister doesn't support custom saga finders.");
            }

            return DynamoDBSagaIdGenerator.Generate(context.SagaMetadata.SagaEntityType, context.CorrelationProperty.Name, context.CorrelationProperty.Value);
        }
    }
}