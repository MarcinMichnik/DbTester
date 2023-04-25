using System.Data.SqlClient;
using System.Diagnostics;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;

namespace DbTester.Executors
{
    public class DeleteAllExecutor : AbstractExecutor, IExecutor
    {
        public DeleteAllExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray) 
        {
            string operationType = "Delete";
            string statement = "DELETE_ALL";
            TryExecuteOperation(result, operationType, statement, () =>
            {
                DeleteAll(result, sourceArray, operationType, statement);
            });
        }

        // Does not commit deletion; Will execute n times and make average
        private void DeleteAll(JObject result, JArray sourceArray, string operationType, string statement)
        {
            double totalTimeTaken = 0;
            Delete deleteQuery = new(_tableName);
            for (int i = 0; i < _executeTimesN; i++)
            {
                double timeTaken = 0;
                using (SqlTransaction transaction = _connection.BeginTransaction())
                {
                    try
                    {
                        // Create and configure the command
                        using (SqlCommand deleteCommand = new(deleteQuery.ToString(TimeZoneInfo.Local), _connection, transaction))
                        {
                            // Measure the time taken to execute the command
                            DateTime before = DateTime.Now;
                            deleteCommand.ExecuteNonQuery();
                            timeTaken = (DateTime.Now - before).TotalMilliseconds;

                            // Roll back the transaction, so the changes are not committed
                            transaction.Rollback();
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle exceptions and roll back the transaction if needed
                        Console.WriteLine($"Error: {ex.Message}");
                        transaction.Rollback();
                    }
                }
                totalTimeTaken += timeTaken;
            }

            result[operationType][statement]["ExecutionTime"] = Math.Round(totalTimeTaken / _executeTimesN, 2);
        }
    }
}
