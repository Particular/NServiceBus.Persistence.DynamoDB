namespace NServiceBus.Persistence.DynamoDB.TransactionalSession.Tests
{
    using NServiceBus.TransactionalSession;
    using NUnit.Framework;
    using Particular.Approvals;
    using PublicApiGenerator;

    [TestFixture]
    public class ApiApprovals
    {
        [Test]
        public void Approve()
        {
            var publicApi = typeof(DynamoOpenSessionOptions).Assembly.GeneratePublicApi(new ApiGeneratorOptions
            {
                ExcludeAttributes = new[] { "System.Runtime.Versioning.TargetFrameworkAttribute", "System.Reflection.AssemblyMetadataAttribute" }
            });
            Approver.Verify(publicApi);
        }
    }
}