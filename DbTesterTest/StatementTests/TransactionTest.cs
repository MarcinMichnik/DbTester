using QueryBuilder.Statements;

namespace QueryBuilderTest.StatementTests
{
    internal class TransactionTest : AbstractTest
    {
        [Test]
        public void TestTransactionWithTwoInserts()
        {
            Transaction query = GetTransactionWithTwoInserts();

            string expected = @$"BEGIN
                                    INSERT INTO {TableName} (
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
                                    );

                                    INSERT INTO {TableName} (
                                        MASTER_ID,
                                        ID,
                                        NAME,
                                        SAVINGS,
                                        MODIFIED_AT,
                                        MODIFIED_BY
                                    ) VALUES (
                                        SEQ.NEXT_VAL,
                                        2,
                                        'HANNAH',
                                        12.1,
                                        {CurrentTimestampCall.Literal},
                                        '{ModifiedBy}'
                                    );
                                END;";

            string actual = query.ToString(TimeZone);
            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        [Test]
        public void TestTransactionWithOneInsertAndOneUpdate()
        {
            Transaction query = GetTransactionWithOneInsertAndOneUpdate();

            string expected = @$"BEGIN
                                    INSERT INTO {TableName} (
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
                                    );

                                    UPDATE {TableName} SET 
                                        NAME = 'HANNAH',
                                        SAVINGS = 12.1,
                                        MODIFIED_AT = {CurrentTimestampCall.Literal},
                                        MODIFIED_BY = '{ModifiedBy}'
                                    WHERE
                                        ID = 1 
                                        AND EXTERNAL_ID = 301;
                                END;";

            string actual = query.ToString(TimeZone);
            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        private Transaction GetTransactionWithTwoInserts()
        {
            Transaction query = new();

            Insert insert1 = GetInsertWithMasterPrimaryKey(1);
            Insert insert2 = GetInsertWithMasterPrimaryKey(2);

            query.AddStatement(insert1);
            query.AddStatement(insert2);

            return query;
        }

        private Transaction GetTransactionWithOneInsertAndOneUpdate()
        {
            Transaction query = new();

            Insert insert = GetInsertWithMasterPrimaryKey(1);
            Update update = GetUpdateWithManyPrimaryKeys();

            query.AddStatement(insert);
            query.AddStatement(update);

            return query;
        }
    }
}
