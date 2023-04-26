using System.Data.SqlClient;
using System.Data;
using DbTester.Statements;
using Newtonsoft.Json.Linq;
using DbTester.Executors;

namespace DbTester
{
    public class DbSimulation
    {
        public List<IExecutor> SqlStatementExecutors { get; set; } = new();
        public SqlConnection Connection { get; set; }
        public string TableName { get; set; }

        private string _filePath;
        private string _dbConnectionString;
        private string _guid;
        private readonly string _executionTimeLiteral = "AverageExecutionTime";

        public DbSimulation(string filePath, string dbConnectionString)
        {
            _filePath = filePath;
            _dbConnectionString = dbConnectionString;
            Connection = new(_dbConnectionString);

            Guid guid = Guid.NewGuid();
            string guidString = guid.ToString();
            _guid = guidString.Replace("-", "_");
            TableName = $"T_TEST_TABLE_{_guid}";
        }
        public string Run()
        {
            // result will be changed during testing
            JObject result = GetResultTemplate();

            JArray sourceArray = ArrayFromSourceFile();
            result["ObjectCount"] = sourceArray.Count;

            Connection.Open();
            PerformTests(result, sourceArray);
            DropTable();
            Connection.Close();

            return result.ToString();
        }

        private void PerformTests(JObject result, JArray sourceArray)
        {
            foreach (IExecutor executor in SqlStatementExecutors)
            {
                executor.Execute(result, sourceArray);
            }

            SumUpTotalJobDuration(result);
        }

        private void SumUpTotalJobDuration(JObject result)
        {
            IEnumerable<JToken> all = result.DescendantsAndSelf();
            IEnumerable<JProperty> allProps = all.OfType<JProperty>();
            IEnumerable<JProperty> allTimes = allProps.Where(prop => prop.Name == _executionTimeLiteral);
            var timeSum = allTimes.Sum(prop => (double)prop.Value);
            result["JobDuration"] = Math.Round(timeSum, 2);
        }

        private JArray ArrayFromSourceFile()
        {
            string sourceFileFullPath = Path.GetFullPath(_filePath);
            string sourceJsonString = File.ReadAllText(sourceFileFullPath);
            // Parse validates whether source data is jarray format
            JArray sourceArray = JArray.Parse(sourceJsonString);
            return sourceArray;
        }

        private void DropTable()
        {
            DropTable dropTableQuery = new(TableName);
            SqlCommand dropTableCommand = new(dropTableQuery.ToString(), Connection);
            dropTableCommand.ExecuteNonQuery();
        }

        private JObject GetResultTemplate()
        {
            JObject result = new();
            result["Status"] = "Success";
            result["TestCount"] = 0;
            result["SuccessfulTests"] = 0;
            result["FailedTests"] = 0;
            result["ObjectCount"] = 0;
            result["JobDuration"] = 0;
            result["Create"] = new JObject()
            {
                { "INSERT_ALL", GetSubObject() },
                { "INSERT_SINGLE", GetSubObject() }
            };
            result["Read"] = new JObject()
            {
                { "SELECT_SINGLE", GetSubObject() },
                { "SELECT_ALL", GetSubObject() }
            };
            result["Update"] = new JObject()
            {
                { "UPDATE_SINGLE", GetSubObject() },
                { "UPDATE_ALL", GetSubObject() }
            };
            result["Delete"] = new JObject()
            {
                { "DELETE_SINGLE", GetSubObject() },
                { "DELETE_ALL", GetSubObject() },
                { "TRUNCATE", GetSubObject() }
            };
            result["Merge"] = new JObject()
            {
                { "MERGE_SINGLE", GetSubObject() },
                { "MERGE_ALL", GetSubObject() }
            };

            return result;
        }

        private JObject GetSubObject()
        {
            return new JObject() {
                    { _executionTimeLiteral, 0 },
                    { "StandardDeviation", 0 },
                    { "Errors", new JArray() } };
        }
    }
}
