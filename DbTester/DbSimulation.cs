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
            JArray result = new();
            string sourceFileFullPath = Path.GetFullPath(_filePath);
            string sourceJsonString = File.ReadAllText(sourceFileFullPath);
            // Parse validates whether source data is jarray format
            JArray sourceArray = JArray.Parse(sourceJsonString);

            SqlConnection connection = new(_dbConnectionString);
            connection.Open();

            CreateOrReplaceTable(sourceArray, connection);

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

            Select selectQuery = new(_tableName, true);
            SqlCommand selectCommand = new(selectQuery.ToString(), connection);
            SqlDataReader reader = selectCommand.ExecuteReader();

            while (reader.Read())
            {
                ReadSingleRow(reader);
            }
            reader.Close();

            DropTable dropTableQuery = new(_tableName);
            SqlCommand dropTableCommand = new(dropTableQuery.ToString(), connection);
            dropTableCommand.ExecuteNonQuery();

            connection.Close();
            return result.ToString();
        }

        private void CreateOrReplaceTable(JArray sourceArray, SqlConnection connection)
        {
            CreateTable createTableQuery = new(_tableName, sourceArray);
            SqlCommand createTableCommand = new(createTableQuery.ToString(), connection);
            try
            {
                createTableCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
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
