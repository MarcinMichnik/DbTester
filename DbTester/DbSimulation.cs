using System.Data.SqlClient;
using System.Data;
using DbTester.Statements;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;
using QueryBuilder.DataTypes;

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
        public string Run()
        {
            string fullPath = Path.GetFullPath(_filePath);
            string jsonString = File.ReadAllText(fullPath);
            JArray input = JArray.Parse(jsonString);
            CreateTable ct = new(_tableName, input);
            string createTableQuery = ct.ToString();

            SqlConnection connection = new(_dbConnectionString);
            connection.Open();
            SqlCommand createTableCommand = new(createTableQuery, connection);
            try 
            {
                createTableCommand.ExecuteNonQuery();
            } catch (Exception ex) 
            {
                DropTable dropTableQueryExc = new(_tableName);
                SqlCommand dropTableCommandExc = new(dropTableQueryExc.ToString(), connection);
                dropTableCommandExc.ExecuteNonQuery();
                createTableCommand.ExecuteNonQuery();
            }
            
            Insert insertQuery = new(_tableName);
            insertQuery.AddColumn("Id", 1);
            insertQuery.AddColumn("Name", "John");
            insertQuery.AddColumn("Salary", 2100);
            insertQuery.AddColumn("DateOfBirth", new SqlFunction("CURRENT_TIMESTAMP"));
            SqlCommand insertCommand = new(insertQuery.ToString(TimeZoneInfo.Local), connection);
            insertCommand.ExecuteNonQuery();

            SqlCommand selectCommand = new($"SELECT * FROM {_tableName}", connection);
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
            return new JArray().ToString();
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
