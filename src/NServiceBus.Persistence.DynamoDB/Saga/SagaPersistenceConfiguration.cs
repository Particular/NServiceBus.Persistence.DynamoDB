namespace NServiceBus.Persistence.DynamoDB
{
    using System;

    /// <summary>
    /// The saga persistence configuration options.
    /// </summary>
    public class SagaPersistenceConfiguration
    {
        /// <summary>
        /// Function used to determine the table name for storing sagas of a given type.
        /// </summary>
        public Func<Type, string> TableNameCallback { get; set; }
    }
}