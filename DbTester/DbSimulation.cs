using System.Data.SqlClient;
using System.Data;
using DbTester.Statements;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;
using QueryBuilder.DataTypes;
using System.Security.Cryptography.X509Certificates;

namespace DbTester
{
    public class DbSimulation
    {
        private string _filePath;
        private string _dbConnectionString;
        private readonly string _tableName;
        public DbSimulation(string filePath, string dbConnectionString)
        {
            _filePath = filePath;
            _dbConnectionString = dbConnectionString;

            Guid guid = Guid.NewGuid();
            string guidString = guid.ToString().Replace("-", "_");
            _tableName = $"T_TEST_TABLE_{guidString}";
        }
        public string Run()
        {
            JObject result = GetResultTemplate();

            JArray sourceArray = ArrayFromSourceFile();
            result["ObjectCount"] = sourceArray.Count;

            SqlConnection connection = new(_dbConnectionString);
            connection.Open();

            CreateOrReplaceTable(sourceArray, connection);

            InsertEach(sourceArray, connection);

            SelectAndRead(connection);

            DropTable(connection);

            connection.Close();

            return result.ToString();
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
                { "INSERT", new JObject() },
                { "INSERT_VIA_MERGE", new JObject() }
            };
            result["Read"] = new JObject()
            {
                { "SELECT_SINGLE", new JObject() },
                { "SELECT_ALL", new JObject() }
            };
            result["Update"] = new JObject()
            {
                { "UPDATE_SINGLE", new JObject() },
                { "UPDATE_ALL", new JObject() },
                { "UPDATE_VIA_MERGE", new JObject() }
            };
            result["Delete"] = new JObject()
            {
                { "DELETE_SINGLE", new JObject() },
                { "DELETE_ALL", new JObject() },
                { "TRUNCATE", new JObject() }
            };
            result["Merge"] = new JObject()
            {
                { "MERGE", new JObject() },
                { "CONDITIONAL_MERGE", new JObject() }
            };

            return result;
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

        private void SelectAndRead(SqlConnection connection)
        {
            Select selectQuery = new(_tableName, true);
            SqlCommand selectCommand = new(selectQuery.ToString(), connection);
            SqlDataReader reader = selectCommand.ExecuteReader();

            while (reader.Read())
            {
                ReadSingleRow(reader);
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
                DropTable dropTableQuery = new(_tableName);
                SqlCommand dropTableCommand = new(dropTableQuery.ToString(), connection);
                dropTableCommand.ExecuteNonQuery();
                createTableCommand.ExecuteNonQuery();
            }
        }

        private void ReadSingleRow(IDataRecord dataRecord)
        {
            for (int i = 0; i < dataRecord.FieldCount; i++)
            {
                Console.Write(dataRecord[i]);
                if (i != dataRecord.FieldCount - 1)
                {
                    Console.Write(", ");
                }
            }
            Console.Write('\n');
        }
    }
}
