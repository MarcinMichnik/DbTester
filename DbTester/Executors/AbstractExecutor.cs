using System.Data;
using System.Data.SqlClient;
using System.Text;
using DbTester.Statements;
using Newtonsoft.Json.Linq;

namespace DbTester.Executors
{
    public abstract class AbstractExecutor
    {
        protected string _tableName;
        protected SqlConnection _connection;
        protected uint _executeTimesN = 64; // Will execute executor n times and make an average val

        public AbstractExecutor(DbSimulation simulation)
        {
            _tableName = simulation.TableName;
            _connection = simulation.Connection;
        }

        protected void TryExecuteOperation(JObject result, string operationType, string statement, Action action)
        {
            result["TestCount"] = (int)result["TestCount"] + 1;
            result["SuccessfulTests"] = (int)result["SuccessfulTests"] + 1;
            try
            {
                action();
            }
            catch (Exception e)
            {
                AddError(result, e.Message, operationType, statement);
            }
        }

        protected void AddError(JObject result, string message, string operationType, string statement)
        {
            JArray errors = (JArray)result[operationType][statement]["Errors"];
            errors.Add(message);
            result["Status"] = "Error";
            result["SuccessfulTests"] = (int)result["SuccessfulTests"] - 1;
            result["FailedTests"] = (int)result["FailedTests"] + 1;
        }

        protected void SelectAndRead(JObject result, Select selectQuery, string operationType, string statement)
        {
            double totalTimeTaken = 0;
            for (int i = 0; i < _executeTimesN; i++)
            {
                double timeTaken = 0;
                using (SqlTransaction transaction = _connection.BeginTransaction())
                {
                    try
                    {
                        // Create and configure the command
                        using (SqlCommand selectCommand = new(selectQuery.ToString(TimeZoneInfo.Local), _connection, transaction))
                        {
                            // Measure the time taken to execute the command
                            DateTime before = DateTime.Now;
                            SqlDataReader reader = selectCommand.ExecuteReader();
                            while (reader.Read())
                            {
                                ReadSingleRow(reader); // no need to log row
                            }
                            reader.Close();
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

        protected string ReadSingleRow(IDataRecord dataRecord)
        {
            StringBuilder sb = new();
            for (int i = 0; i < dataRecord.FieldCount; i++)
            {
                object val = dataRecord[i];
                sb.Append(val.ToString());
                if (i != dataRecord.FieldCount - 1)
                    sb.Append(',');
            }
            sb.Append('\n');
            return sb.ToString();
        }
    }
}
