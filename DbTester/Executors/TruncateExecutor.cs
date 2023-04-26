using System.Data.SqlClient;
using DbTester.Statements;
using Newtonsoft.Json.Linq;

namespace DbTester.Executors
{
    public class TruncateExecutor : AbstractExecutor, IExecutor
    {
        public TruncateExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray)
        {
            string operationType = "Delete";
            string statement = "TRUNCATE";

            TryExecuteOperation(result, operationType, statement, () => 
            {
                Truncate(result, operationType, statement);
            });
        }

        private void Truncate(JObject result, string operationType, string statement) 
        {
            Truncate truncateQuery = new(_tableName);
            List<double> timeList = new();
            for (int i = 0; i < _executeTimesN; i++)
            {
                using (SqlTransaction transaction = _connection.BeginTransaction())
                {
                    try
                    {
                        // Create and configure the command
                        using SqlCommand truncateCommand = new(truncateQuery.ToString(TimeZoneInfo.Local), _connection, transaction);
                        // Measure the time taken to execute the command
                        DateTime before = DateTime.Now;
                        truncateCommand.ExecuteNonQuery();
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
            }

            CalculateTimeValues(result, operationType, statement, timeList);
        }
    }
}
