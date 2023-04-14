using DbTester.DataTypes;
using Newtonsoft.Json.Linq;
using QueryBuilder.DataTypes;
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

            List<KeyValuePair<string, JToken>> columns = new();
            columns.Add(new KeyValuePair<string, JToken>("NAME", "HANNAH"));
            columns.Add(new KeyValuePair<string, JToken>("SAVINGS", 12.1));
            string timestampLiteral = CurrentTimestampCall.GetPrefixedLiteral();
            columns.Add(new KeyValuePair<string, JToken>("MODIFIED_AT", timestampLiteral));
            columns.Add(new KeyValuePair<string, JToken>("MODIFIED_BY", ModifiedBy));
            Row row = new(columns);
            query.AddRow(row);

            query.Where("ID", "=", 1);

            return query;
        }
    }
}
