using Newtonsoft.Json.Linq;
using QueryBuilder.BulkOperations;

namespace QueryBuilderTest.BulkOperationTests
{
    internal class BulkMergeTest : AbstractTest
    {
        private List<string> PrimaryKeyIdentifiers { get; set; } = new() { "ID" };
        private int MaxOperationSize { get; set; } = 1000;

        [Test]
        public void TestSingleMerge()
        {
            int start = 1;
            JArray incoming = GetExampleArrayWithNamedEntity(start, "HANNAH");
            JArray existing = GetExampleArrayWithNamedEntity(start + 1, "HANNAH");

            BulkMerge actual = new(
                incoming, existing, TableName, PrimaryKeyIdentifiers);

            string expected = @$"BEGIN
                                    INSERT INTO {TableName} (
                                        ID,
                                        NAME,
                                        SAVINGS,
                                        MODIFIED_AT,
                                        MODIFIED_BY
                                    ) VALUES (
                                        {start},
                                        'HANNAH',
                                        12.1,
                                        {CurrentTimestampCall.Literal},
                                        '{ModifiedBy}'
                                    );
                                 END;";

            string actualStr = actual.ToString(TestHelpers.TimeZone);
            string actualEscaped = TestHelpers.RemoveWhitespace(actualStr);

            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        [Test]
        public void TestSingleMergeInsertCount()
        {
            int start = 1;
            JArray incoming = GetExampleArrayWithNamedEntity(start, "HANNAH");
            JArray existing = GetExampleArrayWithNamedEntity(start + 1, "HANNAH");

            BulkMerge actual = new(
                incoming, existing, TableName, PrimaryKeyIdentifiers);

            int actualCount = actual.GetInsertCount();

            Assert.That(actualCount, Is.EqualTo(1));
        }

        [Test]
        public void TestBulkMerge()
        {
            int start = 1;
            JArray incoming = GetBigArrayOfEntities(start, MaxOperationSize);
            JArray existing = GetBigArrayOfEntities(start + 1, MaxOperationSize - 1);

            BulkMerge actual = new(
                incoming, existing, TableName, PrimaryKeyIdentifiers);

            string expected = @$"BEGIN
                                    INSERT INTO {TableName} (
                                        ID,
                                        NAME,
                                        MODIFIED_AT,
                                        MODIFIED_BY
                                    ) VALUES (
                                        {start},
                                        'HANNAH',
                                        {CurrentTimestampCall.Literal},
                                        '{ModifiedBy}'
                                    );

                                    INSERT INTO {TableName} (
                                        ID,
                                        NAME,
                                        MODIFIED_AT,
                                        MODIFIED_BY
                                    ) VALUES (
                                        {MaxOperationSize},
                                        'HANNAH',
                                        {CurrentTimestampCall.Literal},
                                        '{ModifiedBy}'
                                    );
                                 END;";

            string actualStr = actual.ToString(TestHelpers.TimeZone);
            string actualEscaped = TestHelpers.RemoveWhitespace(actualStr);

            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        [Test]
        public void TestMergeWithUpdates()
        {
            JArray incoming = GetExampleArrayWithNamedEntity(1, "HANNAH");
            JArray existing = GetExampleArrayWithNamedEntity(1, "SOMEONE");

            BulkMerge actual = new(
                incoming, existing, TableName, PrimaryKeyIdentifiers);

            string expected = @$"BEGIN
                                    UPDATE {TableName} SET 
                                        NAME = 'HANNAH',
                                        SAVINGS = 12.1,
                                        MODIFIED_AT = {CurrentTimestampCall.Literal},
                                        MODIFIED_BY = '{ModifiedBy}'
                                    WHERE ID = 1;
                                 END;";

            string actualStr = actual.ToString(TestHelpers.TimeZone);
            string actualEscaped = TestHelpers.RemoveWhitespace(actualStr);

            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        [Test]
        public void TestBulkMergeWithBigTransactions()
        {
            JArray incoming = GetBigArrayOfEntities(1, MaxOperationSize);

            BulkMerge actualEntity = new(
                incoming, new JArray(), TableName, PrimaryKeyIdentifiers);
            int actual = actualEntity.GetTransactionCount();

            int expected = (int)Math.Ceiling(MaxOperationSize / (double)actualEntity.MaxTransactionSize);

            Assert.That(actual, Is.EqualTo(expected));
        }

        private static JArray GetBigArrayOfEntities(int min, int max)
        {
            JArray incoming = new();
            for (int i = min; i <= max; i++)
            {
                JObject o = new()
                {
                    ["ID"] = i,
                    ["NAME"] = "HANNAH"
                };
                incoming.Add(o);
            }

            return incoming;
        }

        private static JArray GetExampleArrayWithNamedEntity(int id, string entityName)
        {
            JArray existing = new();
            JObject exampleExisting = new()
            {
                ["ID"] = id,
                ["NAME"] = entityName,
                ["SAVINGS"] = 12.1
            };
            existing.Add(exampleExisting);
            return existing;
        }
    }
}
