using DbTester.DataTypes;
using Newtonsoft.Json.Linq;
using QueryBuilder.DataTypes;
using QueryBuilder.Statements;

namespace QueryBuilderTest.StatementTests
{
    internal class InsertTest : AbstractTest
    {
        [Test]
        public void TestInsertWithSequencedMasterPrimaryKey()
        {
            Insert query = GetInsertWithMasterPrimaryKey(1);

            string expected = @$"INSERT INTO {TableName} (
                                    MASTER_ID,
                                    ID,
                                    NAME,
                                    SAVINGS,
                                    MODIFIED_AT,
                                    MODIFIED_BY
                                ) VALUES (
                                    SEQ.NEXT_VAL,
                                    1,
                                    'HANNAH',
                                    12.1,
                                    {CurrentTimestampCall.Literal},
                                    '{ModifiedBy}'
                                );";

            string actual = query.ToString(TimeZone);
            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        [Test]
        public void TestInsertWithoutMasterPrimaryKey()
        {
            Insert query = GetInsertWithoutMasterPrimaryKey();

            string expected = @$"INSERT INTO {TableName} (
                                    ID,
                                    NAME,
                                    SAVINGS,
                                    DATE_FROM,
                                    MODIFIED_AT,
                                    MODIFIED_BY
                                ) VALUES (
                                    1,
                                    'HANNAH',
                                    12.1,
                                    '2022-01-01T00:00:00',
                                    {CurrentTimestampCall.Literal},
                                    '{ModifiedBy}'
                                );";

            string actual = query.ToString(TimeZone);
            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        private Insert GetInsertWithoutMasterPrimaryKey()
        {
            Insert query = new(TableName);
            List<KeyValuePair<string, JToken>> columns = new();
            columns.Add(new KeyValuePair<string, JToken>("ID", 1));
            columns.Add(new KeyValuePair<string, JToken>("NAME", "HANNAH"));
            columns.Add(new KeyValuePair<string, JToken>("SAVINGS", 12.1));
            columns.Add(new KeyValuePair<string, JToken>("DATE_FROM", DateTime.Parse("2022-01-01T00:00:00+01:00")));
            string timestampLiteral = CurrentTimestampCall.GetPrefixedLiteral();
            columns.Add(new KeyValuePair<string, JToken>("MODIFIED_AT", timestampLiteral));
            columns.Add(new KeyValuePair<string, JToken>("MODIFIED_BY", ModifiedBy));
            Row row = new(columns);
            query.AddRow(row);

            return query;
        }
    }
}