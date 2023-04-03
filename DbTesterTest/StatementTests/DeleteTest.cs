using QueryBuilder.Statements;

namespace QueryBuilderTest.StatementTests
{
    internal class DeleteTest : AbstractTest
    {
        [Test]
        public void TestSimpleDelete()
        {
            Delete query = GetSimpleDelete();

            string expected = @$"DELETE FROM {TableName}
                                 WHERE ID = 1 AND MONTH = 8;";

            string actual = query.ToString(TimeZone);
            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        private Delete GetSimpleDelete()
        {
            Delete query = new(TableName);

            query.Where("ID", "=", 1);
            query.Where("MONTH", "=", 8);

            return query;
        }
    }
}
