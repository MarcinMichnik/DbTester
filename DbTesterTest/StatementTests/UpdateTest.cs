using QueryBuilder.Statements;

namespace QueryBuilderTest.StatementTests
{
    internal class UpdateTest : AbstractTest
    {
        [Test]
        public void TestUpdateWithOnePrimaryKey()
        {
            Update query = GetUpdateWithOnePrimaryKey();

            string expected = @$"UPDATE {TableName} SET 
                                    NAME = 'HANNAH',
                                    SAVINGS = 12.1,
                                    MODIFIED_AT = {CurrentTimestampCall.Literal},
                                    MODIFIED_BY = '{ModifiedBy}'
                                WHERE
                                    ID = 1;";

            string actual = query.ToString(TimeZone);
            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected.ToString());

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        [Test]
        public void TestUpdateWithManyPrimaryKeys()
        {
            Update query = GetUpdateWithManyPrimaryKeys();

            string expected = @$"UPDATE {TableName} SET 
                                    NAME = 'HANNAH',
                                    SAVINGS = 12.1,
                                    MODIFIED_AT = {CurrentTimestampCall.Literal},
                                    MODIFIED_BY = '{ModifiedBy}'
                                WHERE
                                    ID = 1 
                                    AND EXTERNAL_ID = 301;";

            string actual = query.ToString(TimeZone);
            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected.ToString());

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        private Update GetUpdateWithOnePrimaryKey()
        {
            Update query = new(TableName);
            query.AddColumn("NAME", "HANNAH");
            query.AddColumn("SAVINGS", 12.1);
            query.AddColumn("MODIFIED_AT", CurrentTimestampCall);
            query.AddColumn("MODIFIED_BY", ModifiedBy);

            query.Where("ID", "=", 1);

            return query;
        }
    }
}
