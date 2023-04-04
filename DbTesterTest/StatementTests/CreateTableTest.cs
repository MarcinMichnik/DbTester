using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using QueryBuilder.Statements;
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
            var actual = GetCreateTableQuery(TableName);

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
            var arr = new JArray()
            { 
                new JObject() 
                {
                    { "ID", 1 },
                    { "NAME", "John" },
                    { "SALARY", 2300.00 },
                    { "DATE_OF_BIRTH", DateTime.Parse("2000-01-01") },
                    { "MODIFIED_AT", DateTime.Parse("2020-03-03") },
                    { "MODIFIED_BY", "source" }
                },
                new JObject() 
                {
                    { "ID", 2 },
                    { "NAME", "Marcus" },
                    { "SALARY", 2100.00 },
                    { "DATE_OF_BIRTH", DateTime.Parse("2000-01-01") },
                    { "MODIFIED_AT", DateTime.Parse("2020-03-03") },
                    { "MODIFIED_BY", "source" }
                },
                new JObject() 
                { 
                    { "ID", 3 },
                    { "NAME", "Peel" },
                    { "SALARY", 1900.00 },
                    { "DATE_OF_BIRTH", DateTime.Parse("2000-01-01") },
                    { "MODIFIED_AT", DateTime.Parse("2020-03-03") },
                    { "MODIFIED_BY", "source" }
                }
            };
            var ct = new CreateTable(tableName, arr);
            return ct.ToString();
        }
    }
}
