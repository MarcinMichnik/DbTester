using System.Data.SqlClient;
using System.Data;
using DbTester.Statements;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;
using DbTester.Commands;
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
            CreateOrReplaceTable(sourceArray);
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

        private static void SumUpTotalJobDuration(JObject result)
        {
            IEnumerable<JToken> all = result.DescendantsAndSelf();
            IEnumerable<JProperty> allProps = all.OfType<JProperty>();
            IEnumerable<JProperty> allTimes = allProps.Where(prop => prop.Name == "ExecutionTime");
            result["JobDuration"] = allTimes.Sum(prop => (int)prop.Value);
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

        private void CreateOrReplaceTable(JArray sourceArray)
        {
            CreateTable createTableQuery = new(TableName, sourceArray);
            SqlCommand createTableCommand = new(createTableQuery.ToString(), Connection);
            try
            {
                createTableCommand.ExecuteNonQuery();
            }
            catch
            {
                DropTable();
                createTableCommand.ExecuteNonQuery();
            }
        }

        private static JObject GetResultTemplate()
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
                { "INSERT_VIA_MERGE", GetSubObject() }
            };
            result["Read"] = new JObject()
            {
                { "SELECT_SINGLE", GetSubObject() },
                { "SELECT_ALL", GetSubObject() }
            };
            result["Update"] = new JObject()
            {
                { "UPDATE_SINGLE", GetSubObject() },
                { "UPDATE_ALL", GetSubObject() },
                { "UPDATE_VIA_MERGE", GetSubObject() }
            };
            result["Delete"] = new JObject()
            {
                { "DELETE_SINGLE", GetSubObject() },
                { "DELETE_ALL", GetSubObject() },
                { "TRUNCATE", GetSubObject() }
            };
            result["Merge"] = new JObject()
            {
                { "MERGE", GetSubObject() },
                { "CONDITIONAL_MERGE", GetSubObject() }
            };

            return result;
        }

        private static JObject GetSubObject()
        {
            return new JObject() {
                    { "ExecutionTime", 0 },
                    { "Errors", new JArray() } };
        }
    }
}
