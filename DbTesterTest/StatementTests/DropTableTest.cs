using DbTester.Statements;
using QueryBuilderTest;

namespace DbTesterTest.StatementTests
{
    internal class DropTableTest : AbstractTest
    {
        [Test]
        public void TestDropTable()
        {
            TableName = "T_TEST_TABLE";
            DropTable query = new(TableName);

            string expected = @$"DROP TABLE {TableName};";

            string actual = query.ToString();
            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }
    }
}
