using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DbTester.DataTypes;
using DbTester.Statements;
using Newtonsoft.Json.Linq;

namespace DbTester.Executors
{
    public class MergeSingleExecutor : AbstractExecutor, IExecutor
    {
        public MergeSingleExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray)
        {
            string operationType = "Merge";
            string statement = "MERGE_SINGLE";
            TryExecuteOperation(result, operationType, statement, () =>
            {
                MergeSingle(result, sourceArray, operationType, statement);
            });
        }

        private void MergeSingle(JObject result, JArray sourceArray, string operationType, string statement)
        {
            Merge mergeQuery = new(_tableName);

            JObject first = (JObject)sourceArray.First();
            List<KeyValuePair<string, JToken>> columns = new();
            foreach (JProperty prop in first.Properties())
            {
                string propName = prop.Name;
                JToken val = prop.Value;
                columns.Add(new KeyValuePair<string, JToken>(propName, val));
            }
            Row row = new(columns);
            mergeQuery.AddRow(row);
            SqlCommand mergeCommand = new(mergeQuery.ToString(TimeZoneInfo.Local), _connection);

            DateTime before = DateTime.Now;
            mergeCommand.ExecuteNonQuery();
            result[operationType][statement]["ExecutionTime"] = (DateTime.Now - before).TotalMilliseconds;
        }
    }
}
