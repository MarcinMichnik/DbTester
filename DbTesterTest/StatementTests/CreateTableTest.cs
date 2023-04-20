using QueryBuilderTest;
using DbTester.Statements;
using Newtonsoft.Json.Linq;

namespace DbTesterTest.StatementTests
{
    internal class CreateTableTest : AbstractTest
    {
        [Test]
        public void TestCreateTableFromJarray()
        {
            string actual = GetCreateTableQuery(TableName);

            string expected = @$"CREATE TABLE {TableName} (
                                    ID INT,
                                    NAME VARCHAR(64),
                                    SALARY FLOAT(53),
                                    DATE_OF_BIRTH DATETIME,
                                    MODIFIED_AT DATETIME,
                                    MODIFIED_BY VARCHAR(64)
                                );";

            string actualEscaped = TestHelpers.RemoveWhitespace(actual);
            string expectedEscaped = TestHelpers.RemoveWhitespace(expected);

            Assert.That(actualEscaped, Is.EqualTo(expectedEscaped));
        }

        private string GetCreateTableQuery(string tableName)
        {
            JArray arr = new()
            { 
                new JObject() 
                {
                    { "ID", 1 },
                    { "NAME", "John" },
                    { "SALARY", 2300.00 },
                    { "DATE_OF_BIRTH", DateTime.Parse("2000-01-01") },
                    { "MODIFIED_AT", DateTime.Parse("2020-03-03") },
                    { "MODIFIED_BY", "source" }
                }
            };
            CreateTable ct = new(tableName, arr);
            return ct.ToString();
        }
    }
}
