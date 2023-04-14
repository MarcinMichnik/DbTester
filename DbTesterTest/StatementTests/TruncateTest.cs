using DbTester.Statements;
using QueryBuilderTest;

namespace DbTesterTest.StatementTests
{
    internal class TruncateTest : AbstractTest
    {
        [Test]
        public void TestSimpleTruncate()
        {
            Truncate query = new(TableName);
            string expected = $@"TRUNCATE TABLE {TableName};";

            string actual = query.ToString(TimeZone);
            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }
    }
}
