using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;

namespace DbTester.Executors
{
    public class InsertAllExecutor : AbstractExecutor, IExecutor
    {
        public InsertAllExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray)
        {
            string operationType = "Create";
            string statement = "INSERT_ALL";
            TryExecuteOperation(result, operationType, statement, () =>
            {
                InsertEach(result, sourceArray, operationType, statement);
            });
        }

        private void InsertEach(JObject result, JArray sourceArray, string operationType, string statement)
        {
            var totalTime = 0;
            foreach (JObject obj in sourceArray.Children<JObject>())
            {
                Insert insertQuery = new(_tableName);
                foreach (JProperty prop in obj.Properties())
                {
                    string propName = prop.Name;
                    JToken val = prop.Value;
                    insertQuery.AddColumn(propName, val);
                }
                SqlCommand insertCommand = new(insertQuery.ToString(TimeZoneInfo.Local), _connection);

                DateTime before = DateTime.Now;
                insertCommand.ExecuteNonQuery();
                totalTime += (DateTime.Now - before).Milliseconds;
            }
            result[operationType][statement]["ExecutionTime"] = totalTime;
        }
    }
}
