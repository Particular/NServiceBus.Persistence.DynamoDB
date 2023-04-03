namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Text.Json;

    /// <summary>
    /// The saga persistence configuration options.
    /// </summary>
    public class SagaPersistenceConfiguration
    {
        /// <summary>
        /// The configuration of the table used by the outbox persistence.
        /// </summary>
        public TableConfiguration Table { get; set; } = new TableConfiguration()
        {
            TimeToLiveAttributeName = null
        };

        /// <summary>
        /// Enables pessimistic locking mode to avoid concurrent modifications to the same saga. Enable this mode to reduce retries due to optimistic concurrency control violations.
        /// </summary>
        public bool UsePessimisticLocking { get; set; } = false;

        /// <summary>
        /// Determines whether the NServiceBus installer should create the Outbox table when enabled.
        /// </summary>
        internal bool CreateTable { get; set; } = true;

        /// <summary>
        /// Defines the lease duration when using pessimistic locking.
        /// </summary>
        public TimeSpan LeaseDuration = TimeSpan.FromSeconds(30); // based on SQS visibility timeout

        /// <summary>
        /// How long the client should attempt to acquire a lock when using pessimistic locking before giving up.
        /// </summary>
        public TimeSpan LeaseAcquisitionTimeout { get; set; } = TimeSpan.FromSeconds(10);

        internal JsonSerializerOptions MapOptions { get; set; } = new(Mapper.MapDefaults);
        internal JsonSerializerOptions ObjectOptions { get; set; } = new(Mapper.ObjectDefaults);
    }
}