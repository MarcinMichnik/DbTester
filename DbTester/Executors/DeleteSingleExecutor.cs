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

        private void DeleteSingle(JObject result, JArray sourceArray,
            string operationType, string statement)
        {
            Delete deleteQuery = new(_tableName);
            JProperty idProp = (JProperty)sourceArray.First().First();
            deleteQuery.Where(idProp.Name, "=", idProp.Value);
            List<double> timeList = new();
            for (int i = 0; i < _executeTimesN; i++)
            {
                using SqlTransaction transaction = _connection.BeginTransaction();
                try
                {
                    // Create and configure the command
                    using SqlCommand deleteCommand = new(deleteQuery.ToString(TimeZoneInfo.Local),
                        _connection, transaction);
                    // Measure the time taken to execute the command
                    DateTime before = DateTime.Now;
                    deleteCommand.ExecuteNonQuery();
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
