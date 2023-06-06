using System.Data.SqlClient;
using DbTester.DataTypes;
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

        private void UpdateAll(JObject result, JArray sourceArray,
            string operationType, string statement)
        {
            Update updateQuery = new(_tableName);
            JObject first = (JObject)sourceArray.First();
            List<KeyValuePair<string, JToken>> columns = new();
            foreach (JProperty prop in first.Properties())
            {
                string propName = prop.Name;
                JToken val = prop.Value;
                columns.Add(new KeyValuePair<string, JToken>(propName, val));
            }
            Row row = new(columns);
            updateQuery.AddRow(row);
            updateQuery.Where("Id", "<>", ""); // One where needs to be used because of underlying impl
            List<double> timeList = new();
            for (int i = 0; i < _executeTimesN; i++)
            {
                using SqlTransaction transaction = _connection.BeginTransaction();
                try
                {
                    // Create and configure the command
                    using SqlCommand updateCommand = new(updateQuery.ToString(TimeZoneInfo.Local),
                        _connection, transaction);
                    // Measure the time taken to execute the command
                    DateTime before = DateTime.Now;
                    updateCommand.ExecuteNonQuery();
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
