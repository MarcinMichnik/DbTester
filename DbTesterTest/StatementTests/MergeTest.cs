using DbTester.DataTypes;
using DbTester.Statements;
using Newtonsoft.Json.Linq;
using QueryBuilder.DataTypes;
using QueryBuilderTest;

namespace DbTesterTest.StatementTests
{
    internal class MergeTest : AbstractTest
    {
        [Test]
        public void TestSimpleMerge()
        {
            Merge query = GetMergeSingle();
            string expected = $@"MERGE INTO {TableName} AS T
                                 USING (VALUES (
                                    1, 
                                    'HANNAH', 
                                    12.1, 
                                    '2022-01-01T00:00:00', 
                                    {CurrentTimestampCall.Literal}, 
                                    '{ModifiedBy}')) AS S (ID, NAME, SAVINGS, DATE_FROM, MODIFIED_AT, MODIFIED_BY)
                                 ON T.ID = S.ID
                                 WHEN MATCHED THEN
                                    UPDATE SET T.ID = S.ID,
                                               T.NAME = S.NAME,
                                               T.SAVINGS = S.SAVINGS,
                                               T.DATE_FROM = S.DATE_FROM,
                                               T.MODIFIED_AT = S.MODIFIED_AT,
                                               T.MODIFIED_BY = S.MODIFIED_BY
                                 WHEN NOT MATCHED BY TARGET THEN
                                    INSERT (ID, NAME, SAVINGS, DATE_FROM, MODIFIED_AT, MODIFIED_BY)
                                    VALUES (S.ID, S.NAME, S.SAVINGS, S.DATE_FROM, S.MODIFIED_AT, S.MODIFIED_BY)
                                 WHEN NOT MATCHED BY SOURCE THEN
                                    DELETE;";

            string actual = query.ToString(TimeZone);
            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        [Test]
        public void TestMultipleMerge()
        {
            Merge query = GetMergeMultiple();
            string expected = $@"MERGE INTO {TableName} AS T
                         USING (VALUES (
                            1, 
                            'HANNAH', 
                            12.1, 
                            '2022-01-01T00:00:00', 
                            {CurrentTimestampCall.Literal}, 
                            '{ModifiedBy}'),
                           (2, 
                            'JOHN', 
                            25.5, 
                            '2022-02-01T00:00:00', 
                            {CurrentTimestampCall.Literal}, 
                            '{ModifiedBy}')) AS S (ID, NAME, SAVINGS, DATE_FROM, MODIFIED_AT, MODIFIED_BY)
                         ON T.ID = S.ID
                         WHEN MATCHED THEN
                            UPDATE SET T.ID = S.ID,
                                       T.NAME = S.NAME,
                                       T.SAVINGS = S.SAVINGS,
                                       T.DATE_FROM = S.DATE_FROM,
                                       T.MODIFIED_AT = S.MODIFIED_AT,
                                       T.MODIFIED_BY = S.MODIFIED_BY
                         WHEN NOT MATCHED BY TARGET THEN
                            INSERT (ID, NAME, SAVINGS, DATE_FROM, MODIFIED_AT, MODIFIED_BY)
                            VALUES (S.ID, S.NAME, S.SAVINGS, S.DATE_FROM, S.MODIFIED_AT, S.MODIFIED_BY)
                         WHEN NOT MATCHED BY SOURCE THEN
                            DELETE;";

            string actual = query.ToString(TimeZone);
            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        private Merge GetMergeSingle()
        {
            Merge query = new(TableName);
            List<KeyValuePair<string, JToken>> columns = new();
            columns.Add(new KeyValuePair<string, JToken>("ID", 1));
            columns.Add(new KeyValuePair<string, JToken>("NAME", "HANNAH"));
            columns.Add(new KeyValuePair<string, JToken>("SAVINGS", 12.1));
            columns.Add(new KeyValuePair<string, JToken>("DATE_FROM", DateTime.Parse("2022-01-01T00:00:00+01:00")));
            SqlFunction func = CurrentTimestampCall;
            string funcLiteral = func.GetPrefixedLiteral();
            columns.Add(new KeyValuePair<string, JToken>("MODIFIED_AT", funcLiteral));
            columns.Add(new KeyValuePair<string, JToken>("MODIFIED_BY", ModifiedBy));
            Row row = new(columns);
            query.AddRow(row);
            return query;
        }

        private Merge GetMergeMultiple()
        {
            Merge query = new(TableName);

            List<KeyValuePair<string, JToken>> columns1 = new();
            columns1.Add(new KeyValuePair<string, JToken>("ID", 1));
            columns1.Add(new KeyValuePair<string, JToken>("NAME", "HANNAH"));
            columns1.Add(new KeyValuePair<string, JToken>("SAVINGS", 12.1));
            columns1.Add(new KeyValuePair<string, JToken>("DATE_FROM", DateTime.Parse("2022-01-01T00:00:00+01:00")));
            columns1.Add(new KeyValuePair<string, JToken>("MODIFIED_AT", CurrentTimestampCall.GetPrefixedLiteral()));
            columns1.Add(new KeyValuePair<string, JToken>("MODIFIED_BY", ModifiedBy));
            Row row1 = new(columns1);

            List<KeyValuePair<string, JToken>> columns2 = new();
            columns2.Add(new KeyValuePair<string, JToken>("ID", 2));
            columns2.Add(new KeyValuePair<string, JToken>("NAME", "JOHN"));
            columns2.Add(new KeyValuePair<string, JToken>("SAVINGS", 25.5));
            columns2.Add(new KeyValuePair<string, JToken>("DATE_FROM", DateTime.Parse("2022-02-01T00:00:00+01:00")));
            columns2.Add(new KeyValuePair<string, JToken>("MODIFIED_AT", CurrentTimestampCall.GetPrefixedLiteral()));
            columns2.Add(new KeyValuePair<string, JToken>("MODIFIED_BY", ModifiedBy));
            Row row2 = new(columns2);

            query.AddRow(row1);
            query.AddRow(row2);

            return query;
        }
    }
}
