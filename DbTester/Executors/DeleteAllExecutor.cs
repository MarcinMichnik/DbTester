using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;

namespace DbTester.Executors
{
    public class DeleteAllExecutor : AbstractExecutor, IExecutor
    {
        public DeleteAllExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray) 
        {
            string operationType = "Delete";
            string statement = "DELETE_ALL";
            TryExecuteOperation(result, operationType, statement, () =>
            {
                DeleteAll(result, sourceArray, operationType, statement);
            });
        }

        private void DeleteAll(JObject result, JArray sourceArray, string operationType, string statement)
        {
            Delete deleteQuery = new(_tableName);
            SqlCommand deleteCommand = new(deleteQuery.ToString(TimeZoneInfo.Local), _connection);

            DateTime before = DateTime.Now;
            deleteCommand.ExecuteNonQuery();
            result[operationType][statement]["ExecutionTime"] = (DateTime.Now - before).TotalMilliseconds;
        }
    }
}
