using System.Data.SqlClient;
using System.Data;
using DbTester.Statements;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;
using System.Linq;
using System.Text.Json.Nodes;

namespace DbTester
{
    public class DbSimulation
    {
        private string _filePath;
        private string _dbConnectionString;
        private readonly string _tableName;
        private readonly bool _silent;
        public DbSimulation(string filePath, string dbConnectionString, bool silent)
        {
            _filePath = filePath;
            _dbConnectionString = dbConnectionString;

            Guid guid = Guid.NewGuid();
            string guidString = guid.ToString().Replace("-", "_");
            _tableName = $"T_TEST_TABLE_{guidString}";

            _silent = silent;
        }
        public string Run()
        {
            // result will be changed during testing
            JObject result = GetResultTemplate();

            JArray sourceArray = ArrayFromSourceFile();
            result["ObjectCount"] = sourceArray.Count;

            SqlConnection connection = new(_dbConnectionString);
            connection.Open();

            CreateOrReplaceTable(sourceArray, connection);

            PerformTests(result, sourceArray, connection);

            DropTable(connection);

            connection.Close();

            return result.ToString();
        }

        private void PerformTests(JObject result, JArray sourceArray, SqlConnection connection)
        {
            DateTime before;

            before = new();
            try 
            {
                InsertEach(sourceArray, connection);
            }
            catch (Exception e)
            {
                AddError(result, e.Message, "Create", "INSERT_ALL");
            }
            result["Create"]["INSERT_ALL"]["ExecutionTime"]
                    = (DateTime.Now - before).Milliseconds;
            result["TestCount"] = (int)result["TestCount"] + 1;

            before = new();
            try
            {
                SelectAndReadAll(connection);
            }
            catch (Exception e)
            {
                AddError(result, e.Message, "Read", "SELECT_ALL");
            }
            result["Read"]["SELECT_ALL"]["ExecutionTime"]
                = (DateTime.Now - before).Milliseconds;
            result["TestCount"] = (int)result["TestCount"] + 1;

            before = new();
            try
            {
                SelectAndReadSingle(sourceArray, connection);
            }
            catch (Exception e)
            {
                AddError(result, e.Message, "Read", "SELECT_SINGLE");
            }
            result["Read"]["SELECT_SINGLE"]["ExecutionTime"]
                = (DateTime.Now - before).Milliseconds;
            result["TestCount"] = (int)result["TestCount"] + 1;

            SumUpTotalJobDuration(result);
        }

        private void AddError(JObject result, string message, string operationType, string statement)
        {
            JArray errors = (JArray)result[operationType][statement]["Errors"];
            errors.Add(message);
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

        private void InsertEach(JArray sourceArray, SqlConnection connection)
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
                SqlCommand insertCommand = new(insertQuery.ToString(TimeZoneInfo.Local), connection);
                insertCommand.ExecuteNonQuery();
            }
        }

        private void SelectAndReadAll(SqlConnection connection)
        {
            Select selectQuery = new(_tableName, selectAllFields: true);
            SqlCommand selectCommand = new(selectQuery.ToString(), connection);
            SqlDataReader reader = selectCommand.ExecuteReader();

            while (reader.Read())
            {
                ReadSingleRow(reader, silent: _silent);
            }
            reader.Close();
        }

        private void SelectAndReadSingle(JArray sourceArray, SqlConnection connection)
        {
            Select selectQuery = new(_tableName, selectAllFields: true);
            JProperty id = (JProperty)sourceArray.First().First();
            selectQuery.Where(id.Name, "=", id.Value);
            SqlCommand selectCommand = new(selectQuery.ToString(), connection);
            SqlDataReader reader = selectCommand.ExecuteReader();

            while (reader.Read())
            {
                ReadSingleRow(reader, silent: _silent);
            }
            reader.Close();
        }

        private void DropTable(SqlConnection connection)
        {
            DropTable dropTableQuery = new(_tableName);
            SqlCommand dropTableCommand = new(dropTableQuery.ToString(), connection);
            dropTableCommand.ExecuteNonQuery();
        }

        private void CreateOrReplaceTable(JArray sourceArray, SqlConnection connection)
        {
            CreateTable createTableQuery = new(_tableName, sourceArray);
            SqlCommand createTableCommand = new(createTableQuery.ToString(), connection);
            try
            {
                createTableCommand.ExecuteNonQuery();
            }
            catch
            {
                DropTable(connection);
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

        private void ReadSingleRow(IDataRecord dataRecord, bool silent)
        {
            for (int i = 0; i < dataRecord.FieldCount; i++)
            {
                object val = dataRecord[i];
                if (!silent)
                    Console.Write(val);
                if (!silent && i != dataRecord.FieldCount - 1 )
                {
                    Console.Write(',');
                }
            }
            if (!silent)
                Console.Write('\n');
        }
    }
}
