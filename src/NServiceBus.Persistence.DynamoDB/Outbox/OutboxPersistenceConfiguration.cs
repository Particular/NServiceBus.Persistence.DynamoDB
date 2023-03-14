namespace NServiceBus.Persistence.DynamoDB
{
    using System;

    /// <summary>
    /// The Outbox persistence configuration options.
    /// </summary>
    public class OutboxPersistenceConfiguration
    {
        /// <summary>
        /// The configuration of the table used by the outbox persistence.
        /// </summary>
        public DynamoTableConfiguration TableConfiguration { get; set; } = new DynamoTableConfiguration
        {
            TimeToLiveAttributeName = "ExpireAt"
        };

        /// <summary>
        /// The Time to Live for outbox records.
        /// </summary>
        public TimeSpan TimeToLive { get; set; } = TimeSpan.FromDays(7);

        /// <summary>
        /// Determines whether the NServiceBus installer should create the Outbox table when enabled.
        /// </summary>
        internal bool CreateTable { get; set; } = true;
    }
}