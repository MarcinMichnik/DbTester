using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;

namespace DbTester.Executors
{
    public class DeleteSingleExecutor : AbstractExecutor, IExecutor
    {
        public DeleteSingleExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray)
        {
            string operationType = "Delete";
            string statement = "DELETE_SINGLE";
            TryExecuteOperation(result, operationType, statement, () =>
            {
                DeleteSingle(result, sourceArray, operationType, statement);
            });
        }

        private void DeleteSingle(JObject result, JArray sourceArray, string operationType, string statement)
        {
            Delete deleteQuery = new(_tableName);

            JProperty idProp = (JProperty)sourceArray.First().First();
            deleteQuery.Where(idProp.Name, "=", idProp.Value);

            SqlCommand deleteCommand = new(deleteQuery.ToString(TimeZoneInfo.Local), _connection);

            DateTime before = DateTime.Now;
            deleteCommand.ExecuteNonQuery();
            result[operationType][statement]["ExecutionTime"] = (DateTime.Now - before).TotalMilliseconds;
        }
    }
}
