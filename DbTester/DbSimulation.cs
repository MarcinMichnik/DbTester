using System.Data.SqlClient;
using System.Data;
using DbTester.Statements;
using Newtonsoft.Json.Linq;

namespace DbTester
{
    public class DbSimulation
    {
        private string _filePath;
        private string _dbConnectionString;
        private readonly string _tableName = "T_TEST_TABLE";
        public DbSimulation(string filePath, string dbConnectionString)
        {
            _filePath = filePath;
            _dbConnectionString = dbConnectionString;
        }
        public void Run()
        {
            string fullPath = Path.GetFullPath(_filePath);
            string jsonString = File.ReadAllText(fullPath);
            JArray input = JArray.Parse(jsonString);
            CreateTable ct = new CreateTable(_tableName, input);
            string createTableQuery = ct.ToString();

            SqlConnection connection = new SqlConnection(_dbConnectionString);
            connection.Open();
            SqlCommand command = new SqlCommand(createTableQuery, connection);
            command.ExecuteNonQuery();

            SqlCommand command3 = new SqlCommand(
                $"INSERT INTO {_tableName} VALUES (1, 'John', 2100, CURRENT_TIMESTAMP)", connection);
            command3.ExecuteNonQuery();

            SqlCommand command2 = new SqlCommand($"SELECT * FROM {_tableName}", connection);
            SqlDataReader reader = command2.ExecuteReader();

            while (reader.Read())
            {
                ReadSingleRow(reader);
            }
            reader.Close();

            DropTable dropTableQuery = new(_tableName);
            SqlCommand command4 = new(dropTableQuery.ToString(), connection);
            command4.ExecuteNonQuery();

            connection.Close();
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
        }
    }
}
