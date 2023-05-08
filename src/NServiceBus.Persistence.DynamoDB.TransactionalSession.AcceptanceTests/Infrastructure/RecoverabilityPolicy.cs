namespace NServiceBus.TransactionalSession.AcceptanceTests;

using System;
using Persistence.DynamoDB;
using Transport;

static class RecoverabilityPolicy
{
    public static RecoverabilityAction Invoke(RecoverabilityConfig config, ErrorContext errorContext)
    {
        if (errorContext.Exception is PartialOutboxResultException && errorContext.DelayedDeliveriesPerformed < 3)
        {
            return RecoverabilityAction.DelayedRetry(TimeSpan.FromSeconds(errorContext.DelayedDeliveriesPerformed * 1));
        }
        return DefaultRecoverabilityPolicy.Invoke(config, errorContext);
    }
}