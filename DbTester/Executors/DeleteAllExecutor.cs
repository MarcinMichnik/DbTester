using System.Data.SqlClient;
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
            List<double> timeList = new();
            Delete deleteQuery = new(_tableName);
            for (int i = 0; i < _executeTimesN; i++)
            {
                using SqlTransaction transaction = _connection.BeginTransaction();
                try
                {
                    using SqlCommand deleteCommand = new(deleteQuery.ToString(TimeZoneInfo.Local), _connection, transaction);
                    DateTime before = DateTime.Now;
                    deleteCommand.ExecuteNonQuery();
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
