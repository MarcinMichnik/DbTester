using DbTester.Statements;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Data.SqlClient;

string jsonFilePath = @"..\..\..\Entries.json";
string fullPath = Path.GetFullPath(jsonFilePath);
string jsonString = File.ReadAllText(fullPath);
JArray input = JArray.Parse(jsonString);
string tableName = "T_TEST_TABLE";
CreateTable ct = new CreateTable(tableName);
string query = ct.FromJArray(input);

SqlConnection connection = new SqlConnection(
    "Data Source=localhost;Initial Catalog=master;Integrated Security=True");
connection.Open();
SqlCommand command = new SqlCommand(query, connection);
command.ExecuteNonQuery();

SqlCommand command3 = new SqlCommand($"INSERT INTO {tableName} VALUES (1, 'John', 2100, CURRENT_TIMESTAMP)", connection);
command3.ExecuteNonQuery();

SqlCommand command2 = new SqlCommand($"SELECT * FROM {tableName}", connection);
SqlDataReader reader = command2.ExecuteReader();

while (reader.Read())
{
    ReadSingleRow((IDataRecord)reader);
}
reader.Close();

SqlCommand command4 = new SqlCommand($"DROP TABLE {tableName}", connection);
command4.ExecuteNonQuery();

connection.Close();
void ReadSingleRow(IDataRecord dataRecord)
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