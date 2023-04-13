using System.Data.SqlClient;
using System.Data;
using DbTester.Statements;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;
using DbTester.Commands;

namespace DbTester
{
    public class DbSimulation
    {
        private string _filePath;
        private string _dbConnectionString;
        private SqlConnection _connection;
        private string _guid;
        private readonly string _tableName;
        private readonly bool _silent;
        public DbSimulation(string filePath, string dbConnectionString, bool silent)
        {
            _filePath = filePath;
            _dbConnectionString = dbConnectionString;
            _connection = new(_dbConnectionString);

            Guid guid = Guid.NewGuid();
            string guidString = guid.ToString();
            _guid = guidString.Replace("-", "_");
            _tableName = $"T_TEST_TABLE_{_guid}";

            _silent = silent;
        }
        public string Run()
        {
            // result will be changed during testing
            JObject result = GetResultTemplate();

            JArray sourceArray = ArrayFromSourceFile();
            result["ObjectCount"] = sourceArray.Count;

            _connection.Open();
            CreateOrReplaceTable(sourceArray);
            PerformTests(result, sourceArray);
            DropTable();
            _connection.Close();

            return result.ToString();
        }

        private void PerformTests(JObject result, JArray sourceArray)
        {
            TestInsertAll(result, sourceArray);
            TestSelectAll(result);
            TestSelectSingle(result, sourceArray);
            new UpdateSingleExecutor(_tableName, _connection).Execute(result, sourceArray);

            SumUpTotalJobDuration(result);
        }

        private void TestInsertAll(JObject result, JArray sourceArray)
        {
            TryExecuteOperation(result, "Create", "INSERT_ALL", () =>
            {
                InsertEach(sourceArray);
            });
        }

        private void TestSelectAll(JObject result)
        {
            TryExecuteOperation(result, "Read", "SELECT_ALL", () =>
            {
                SelectAndReadAll();
            });
        }

        private void TestSelectSingle(JObject result, JArray sourceArray)
        {
            TryExecuteOperation(result, "Read", "SELECT_SINGLE", () =>
            {
                SelectAndReadSingle(sourceArray);
            });
        }

        private void TryExecuteOperation(JObject result, string operationType, string statement, Action action)
        {
            result["TestCount"] = (int)result["TestCount"] + 1;
            result["SuccessfulTests"] = (int)result["SuccessfulTests"] + 1;
            DateTime before = DateTime.Now;
            try
            {
                action();
            }
            catch (Exception e)
            {
                AddError(result, e.Message, operationType, statement);
            }
            result[operationType][statement]["ExecutionTime"] = (DateTime.Now - before).Milliseconds;
        }

        private void AddError(JObject result, string message, string operationType, string statement)
        {
            JArray errors = (JArray)result[operationType][statement]["Errors"];
            errors.Add(message);
            result["Status"] = "Error";
            result["SuccessfulTests"] = (int)result["SuccessfulTests"] - 1;
            result["FailedTests"] = (int)result["FailedTests"] + 1;
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

        private void InsertEach(JArray sourceArray)
        {
            foreach (JObject obj in sourceArray.Children<JObject>())
            {
                Insert insertQuery = new(_tableName);
                foreach (JProperty prop in obj.Properties())
                {
                    string propName = prop.Name;
                    JToken val = prop.Value;
                    insertQuery.AddColumn(propName, val);
                }
                SqlCommand insertCommand = new(insertQuery.ToString(TimeZoneInfo.Local), _connection);
                insertCommand.ExecuteNonQuery();
            }
        }

        private void SelectAndReadAll()
        {
            Select selectQuery = new(_tableName, selectAllFields: true);
            SelectAndRead(selectQuery);
        }

        private void SelectAndReadSingle(JArray sourceArray)
        {
            Select selectQuery = new(_tableName, selectAllFields: true);
            JProperty id = (JProperty)sourceArray.First().First();
            selectQuery.Where(id.Name, "=", id.Value);
            SelectAndRead(selectQuery);
        }

        private void SelectAndRead(Select selectQuery)
        {
            SqlCommand selectCommand = new(selectQuery.ToString(), _connection);

            SqlDataReader reader = selectCommand.ExecuteReader();
            while (reader.Read())
            {
                ReadSingleRow(reader);
            }
            reader.Close();
        }

        private void DropTable()
        {
            DropTable dropTableQuery = new(_tableName);
            SqlCommand dropTableCommand = new(dropTableQuery.ToString(), _connection);
            dropTableCommand.ExecuteNonQuery();
        }

        private void CreateOrReplaceTable(JArray sourceArray)
        {
            CreateTable createTableQuery = new(_tableName, sourceArray);
            SqlCommand createTableCommand = new(createTableQuery.ToString(), _connection);
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

        private JObject GetSubObject()
        {
            return new JObject() {
                    { "ExecutionTime", 0 },
                    { "Errors", new JArray() } };
        }

        private void ReadSingleRow(IDataRecord dataRecord)
        {
            for (int i = 0; i < dataRecord.FieldCount; i++)
            {
                object val = dataRecord[i];
                if (!_silent)
                    Console.Write(val);
                if (!_silent && i != dataRecord.FieldCount - 1 )
                    Console.Write(',');
            }
            if (!_silent)
                Console.Write('\n');
        }
    }
}
