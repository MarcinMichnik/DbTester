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

        private void MergeAll(JObject result, JArray sourceArray,
            string operationType, string statement)
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

            List<double> timeList = new();
            for (int i = 0; i < _executeTimesN; i++)
            {
                using SqlTransaction transaction = _connection.BeginTransaction();
                try
                {
                    // Create and configure the command
                    using SqlCommand mergeCommand = new(
                        mergeQuery.ToString(TimeZoneInfo.Local),
                        _connection, transaction);
                    // Measure the time taken to execute the command
                    DateTime before = DateTime.Now;
                    mergeCommand.ExecuteNonQuery();
                    TimeSpan timeTaken = DateTime.Now - before;
                    timeList.Add(timeTaken.TotalMilliseconds);

                    // Roll back the transaction, so the changes are not committed
                    transaction.Rollback();
                }
                catch (Exception ex)
                {
                    // Handle exceptions and roll back the transaction if needed
                    Console.WriteLine($"Error: {ex.Message}");
                    transaction.Rollback();
                }
            }

            CalculateTimeValues(result, operationType, statement, timeList);
        }
    }
}
