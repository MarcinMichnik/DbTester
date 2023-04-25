using System.Data.SqlClient;
using DbTester.Statements;
using Newtonsoft.Json.Linq;

namespace DbTester.Executors
{
    public class CreateOrReplaceTableExecutor : AbstractExecutor, IExecutor
    {
        public CreateOrReplaceTableExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray) 
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

        private void DropTable()
        {
            DropTable dropTableQuery = new(_tableName);
            SqlCommand dropTableCommand = new(dropTableQuery.ToString(), _connection);
            dropTableCommand.ExecuteNonQuery();
        }
    }
}
