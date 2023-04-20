using DbTester.DataTypes;
using DbTester.Statements;
using Newtonsoft.Json.Linq;
using System.Data.SqlClient;

namespace DbTester.Executors
{
    public class MergeAllExecutor : AbstractExecutor, IExecutor
    {
        public MergeAllExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray)
        {
            string operationType = "Merge";
            string statement = "MERGE_ALL";
            TryExecuteOperation(result, operationType, statement, () =>
            {
                MergeAll(result, sourceArray, operationType, statement);
            });
        }

        private void MergeAll(JObject result, JArray sourceArray, string operationType, string statement)
        {
            Merge mergeQuery = new(_tableName);

            foreach (JObject jObject in sourceArray)
            {
                List<KeyValuePair<string, JToken>> columns = new();
                foreach (JProperty prop in jObject.Properties())
                {
                    string propName = prop.Name;
                    JToken val = prop.Value;
                    columns.Add(new KeyValuePair<string, JToken>(propName, val));
                }
                Row row = new(columns);
                mergeQuery.AddRow(row);
            }

            SqlCommand mergeCommand = new(mergeQuery.ToString(TimeZoneInfo.Local), _connection);

            DateTime before = DateTime.Now;
            mergeCommand.ExecuteNonQuery();
            result[operationType][statement]["ExecutionTime"] = (DateTime.Now - before).TotalMilliseconds;
        }
    }
}
