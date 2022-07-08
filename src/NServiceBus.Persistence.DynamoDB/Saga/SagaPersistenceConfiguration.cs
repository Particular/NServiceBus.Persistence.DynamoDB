namespace NServiceBus.Persistence.DynamoDB
{
    /// <summary>
    /// The saga persistence configuration options.
    /// </summary>
    public class SagaPersistenceConfiguration
    {
        /// <summary>
        /// Table name for all sagas
        /// </summary>
        public string TableName { get; set; }
    }
}