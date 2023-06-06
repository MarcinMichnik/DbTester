using System.Data.SqlClient;
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

        private void MergeSingle(JObject result, JArray sourceArray,
            string operationType, string statement)
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

            List<double> timeList = new();
            for (int i = 0; i < _executeTimesN; i++)
            {
                using SqlTransaction transaction = _connection.BeginTransaction();
                try
                {
                    using SqlCommand mergeCommand = new(
                        mergeQuery.ToString(TimeZoneInfo.Local),
                        _connection, transaction);
                    DateTime before = DateTime.Now;
                    mergeCommand.ExecuteNonQuery();
                    TimeSpan timeTaken = DateTime.Now - before;
                    timeList.Add(timeTaken.TotalMilliseconds);

                    // Roll back the transaction, so the changes are not committed
                    transaction.Rollback();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error: {ex.Message}");
                    transaction.Rollback();
                }
            }

            CalculateTimeValues(result, operationType, statement, timeList);
        }
    }
}
