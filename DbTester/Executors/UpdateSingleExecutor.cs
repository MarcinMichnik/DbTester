using System.Data.SqlClient;
using DbTester.DataTypes;
using DbTester.Executors;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;

namespace DbTester.Commands
{
    public class UpdateSingleExecutor : AbstractExecutor, IExecutor
    {
        public UpdateSingleExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray)
        {
            string operationType = "Update";
            string statement = "UPDATE_SINGLE";
            TryExecuteOperation(result, operationType, statement, () =>
            {
                UpdateSingle(result, sourceArray, operationType, statement);
            });
        }

        private void UpdateSingle(JObject result, JArray sourceArray, string operationType, string statement)
        {
            Update updateQuery = new(_tableName);
            JProperty idProp = (JProperty)sourceArray.First().First();
            JObject firstObject = (JObject)sourceArray.First();
            List<KeyValuePair<string, JToken>> columns = new();
            foreach (JProperty? prop in firstObject.Properties())
            {
                if (prop is null)
                    continue;
                columns.Add(new KeyValuePair<string, JToken>(prop.Name, prop.Value));
            }
            Row row = new(columns);
            updateQuery.AddRow(row);
            updateQuery.Where(idProp.Name, "=", idProp.Value);
            List<double> timeList = new();
            for (int i = 0; i < _executeTimesN; i++)
            {
                using SqlTransaction transaction = _connection.BeginTransaction();
                try
                {
                    // Create and configure the command
                    using SqlCommand updateCommand = new(updateQuery.ToString(TimeZoneInfo.Local), _connection, transaction);
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
