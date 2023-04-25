using System.Data.SqlClient;
using DbTester.DataTypes;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;

namespace DbTester.Executors
{
    public class InsertSingleExecutor : AbstractExecutor, IExecutor
    {
        public InsertSingleExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray)
        {
            string operationType = "Create";
            string statement = "INSERT_SINGLE";
            TryExecuteOperation(result, operationType, statement, () =>
            {
                InsertSingle(result, sourceArray, operationType, statement);
            });
        }

        private void InsertSingle(JObject result, JArray sourceArray, string operationType, string statement) 
        {
            Insert insertQuery = new(_tableName);

            JObject first = (JObject)sourceArray.First();
            List<KeyValuePair<string, JToken>> columns = new();
            foreach (JProperty prop in first.Properties())
            {
                string propName = prop.Name;
                JToken val = prop.Value;
                columns.Add(new KeyValuePair<string, JToken>(propName, val));
            }
            Row row = new(columns);
            insertQuery.AddRow(row);

            double totalTimeTaken = 0;
            for (int i = 0; i < _executeTimesN; i++)
            {
                double timeTaken = 0;
                using (SqlTransaction transaction = _connection.BeginTransaction())
                {
                    try
                    {
                        // Create and configure the command
                        using (SqlCommand insertCommand = new(insertQuery.ToString(TimeZoneInfo.Local), _connection, transaction))
                        {
                            // Measure the time taken to execute the command
                            DateTime before = DateTime.Now;
                            insertCommand.ExecuteNonQuery();
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
