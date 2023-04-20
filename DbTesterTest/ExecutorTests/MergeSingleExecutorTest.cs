using DbTester;
using DbTester.Executors;
using Moq;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using System;
using System.Data.Common;
using System.Data.SqlClient;


namespace DbTesterTest.ExecutorTests
{
    [TestFixture]
    public class MergeSingleExecutorTests
    {
        private const string TableName = "TestTable";

        //[Test]
        //public void TestExecute()
        //{
        //    // Arrange
        //    Mock<DbConnection> mockConnection = new();
        //    Mock<DbCommand> mockCommand = new();

        //    mockConnection.Setup(m => ((SqlConnection)m).CreateCommand()).Returns((SqlCommand)mockCommand.Object);
        //    mockCommand.SetupSet(m => m.CommandText = It.IsAny<string>());
        //    mockCommand.SetupSet(m => m.Connection = It.IsAny<DbConnection>());
        //    mockCommand.Setup(m => m.ExecuteNonQuery()).Returns(1);

        //    Mock<DbSimulation> mockSimulation = new();
        //    mockSimulation.SetupGet(m => m.TableName).Returns(TableName);
        //    mockSimulation.SetupGet(m => m.Connection).Returns((SqlConnection)mockConnection.Object);

        //    JObject result = new();
        //    JArray sourceArray = new()
        //    {
        //        new JObject
        //        {
        //            { "ID", 1 },
        //            { "NAME", "HANNAH" },
        //            { "SAVINGS", 12.1 },
        //            { "DATE_FROM", DateTime.Parse("2022-01-01T00:00:00+01:00") },
        //            { "MODIFIED_AT", DateTime.UtcNow },
        //            { "MODIFIED_BY", "test" }
        //        }
        //    };

        //    MergeSingleExecutor executor = new(mockSimulation.Object);

        //    // Act
        //    executor.Execute(result, sourceArray);

        //    // Assert
        //    Assert.IsTrue(result.HasValues);
        //    Assert.IsTrue(result.Properties().Any(p => p.Name == "Merge"));
        //    Assert.IsTrue(result["Merge"].HasValues);
        //    Assert.IsTrue(((JObject)result["Merge"]).Properties().Any(p => p.Name == "MERGE_SINGLE"));
        //    Assert.IsTrue(result["Merge"]["MERGE_SINGLE"].HasValues);
        //    Assert.IsNotNull(result["Merge"]["MERGE_SINGLE"]["ExecutionTime"]);

        //    mockCommand.VerifySet(m => m.CommandText = It.IsAny<string>(), Times.Once);
        //    mockCommand.VerifySet(m => m.Connection = It.IsAny<DbConnection>(), Times.Once);
        //    mockCommand.Verify(m => m.ExecuteNonQuery(), Times.Once);
        //}
    }
}
