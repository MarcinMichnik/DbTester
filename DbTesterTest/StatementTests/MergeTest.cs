using DbTester.Statements;
using QueryBuilderTest;

namespace DbTesterTest.StatementTests
{
    internal class MergeTest : AbstractTest
    {
        //[Test]
        //public void TestSimpleMerge()
        //{
        //    Merge query = GetMergeSingle();
        //    string expected = $@"MERGE INTO {TableName} AS T
        //                         USING (VALUES (
        //                            1, 
        //                            'HANNAH', 
        //                            12.1, 
        //                            '2022-01-01T00:00:00', 
        //                            {CurrentTimestampCall.Literal}, 
        //                            '{ModifiedBy}')) AS S (ID, NAME, SAVINGS, DATE_FROM, MODIFIED_AT, MODIFIED_BY)
        //                         ON T.ID = S.ID
        //                         WHEN MATCHED THEN
        //                            UPDATE SET T.ID = S.ID,
        //                                       T.NAME = S.NAME,
        //                                       T.SAVINGS = S.SAVINGS,
        //                                       T.DATE_FROM = S.DATE_FROM,
        //                                       T.MODIFIED_AT = S.MODIFIED_AT,
        //                                       T.MODIFIED_BY = S.MODIFIED_BY
        //                         WHEN NOT MATCHED BY TARGET THEN
        //                            INSERT (ID, NAME, SAVINGS, DATE_FROM, MODIFIED_AT, MODIFIED_BY)
        //                            VALUES (S.ID, S.NAME, S.SAVINGS, S.DATE_FROM, S.MODIFIED_AT, S.MODIFIED_BY)
        //                         WHEN NOT MATCHED BY SOURCE THEN
        //                            DELETE;";

        //    string actual = query.ToString(TimeZone);
        //    string actualEscaped = TestHelpers.RemoveWhitespace(actual);
        //    string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

        //    Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        //}

        //private Merge GetMergeSingle()
        //{
        //    Merge query = new(TableName);
        //    query.AddColumn("ID", 1);
        //    query.AddColumn("NAME", "HANNAH");
        //    query.AddColumn("SAVINGS", 12.1);
        //    query.AddColumn("DATE_FROM", DateTime.Parse("2022-01-01T00:00:00+01:00"));
        //    query.AddColumn("MODIFIED_AT", CurrentTimestampCall);
        //    query.AddColumn("MODIFIED_BY", ModifiedBy);
        //    return query;
        //}
    }
}
