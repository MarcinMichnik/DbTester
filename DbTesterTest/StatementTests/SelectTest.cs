using DbTester.Statements;
using QueryBuilderTest;

namespace DbTesterTest.StatementTests
{
    internal class SelectTest : AbstractTest
    {
        [Test]
        public void TestSelectAllFromTable()
        {
            TableName = "T_TEST_TABLE";
            Select selectQuery = new(TableName, true);
            string actual = selectQuery.ToString();

            string expected = $"SELECT * FROM {TableName};";

            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        [Test]
        public void TestSelectAllFromTableWhereIdOne()
        {
            TableName = "T_TEST_TABLE";
            Select selectQuery = new(TableName, true);
            selectQuery.Where("ID", "=", 1);
            string actual = selectQuery.ToString();

            string expected = $"SELECT * FROM {TableName} WHERE ID = 1;";

            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        [Test]
        public void TestSelectIdFromTableWhereIdOne()
        {
            TableName = "T_TEST_TABLE";
            Select selectQuery = new(TableName, false);
            selectQuery.AddColumn("NAME");
            selectQuery.Where("ID", "=", 1);
            string actual = selectQuery.ToString();

            string expected = $"SELECT NAME FROM {TableName} WHERE ID = 1;";

            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }
    }
}
