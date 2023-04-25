using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DbTester.DataTypes;
using DbTester.Statements;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;

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

            double totalTimeTaken = 0;
            for (int i = 0; i < _executeTimesN; i++)
            {
                double timeTaken = 0;
                using (SqlTransaction transaction = _connection.BeginTransaction())
                {
                    try
                    {
                        // Create and configure the command
                        using (SqlCommand mergeCommand = new(mergeQuery.ToString(TimeZoneInfo.Local), _connection, transaction))
                        {
                            // Measure the time taken to execute the command
                            DateTime before = DateTime.Now;
                            mergeCommand.ExecuteNonQuery();
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
