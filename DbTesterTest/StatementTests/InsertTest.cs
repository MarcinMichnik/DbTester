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
                                    TO_DATE('2022-01-01""T""00:00:00', 'YYYY-MM-DD""T""HH24:MI:SS'),
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

            query.AddColumn("ID", 1);
            query.AddColumn("NAME", "HANNAH");
            query.AddColumn("SAVINGS", 12.1);
            query.AddColumn("DATE_FROM", DateTime.Parse("2022-01-01T00:00:00+01:00"));
            query.AddColumn("MODIFIED_AT", CurrentTimestampCall);
            query.AddColumn("MODIFIED_BY", ModifiedBy);

            return query;
        }
    }
}