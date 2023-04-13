using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;

namespace DbTester.Executors
{
    public class UpdateAllExecutor : AbstractExecutor, IExecutor
    {
        public UpdateAllExecutor(DbSimulation simulation) : base(simulation) { }
        public void Execute(JObject result, JArray sourceArray)
        {
            string operationType = "Update";
            string statement = "UPDATE_ALL";
            TryExecuteOperation(result, operationType, statement, () =>
            {
                UpdateAll(result, sourceArray, operationType, statement);
            });
        }

        private void UpdateAll(JObject result, JArray sourceArray, string operationType, string statement)
        {
            Update updateQuery = new(_tableName);
            JObject first = (JObject)sourceArray.First();
            foreach (JProperty prop in first.Properties())
            {
                string propName = prop.Name;
                JToken val = prop.Value;
                updateQuery.AddColumn(propName, val);
            }
            updateQuery.Where("Id", "<>", ""); // One where needs to be used because of underlying impl

            SqlCommand updateCommand = new(updateQuery.ToString(TimeZoneInfo.Local), _connection);

            DateTime before = DateTime.Now;
            updateCommand.ExecuteNonQuery();
            result[operationType][statement]["ExecutionTime"] = (DateTime.Now - before).TotalMilliseconds;
        }
    }
}
