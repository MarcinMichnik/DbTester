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
        protected uint _executeTimesN = 512; // Will execute executor n times and make an average val
        protected readonly string _executionTimeLiteral = "AverageExecutionTime";
        protected readonly string _standardDeviationLiteral = "StandardDeviation";

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

        protected void SelectAndRead(JObject result, Select selectQuery,
            string operationType, string statement)
        {
            List<double> timeList = new();
            for (int i = 0; i < _executeTimesN; i++)
            {
                using SqlTransaction transaction = _connection.BeginTransaction();
                try
                {
                    // Create and configure the command
                    using SqlCommand selectCommand = new(selectQuery.ToString(TimeZoneInfo.Local),
                        _connection, transaction);
                    // Measure the time taken to execute the command
                    DateTime before = DateTime.Now;
                    SqlDataReader reader = selectCommand.ExecuteReader();
                    while (reader.Read())
                    {
                        ReadSingleRow(reader); // no need to log row
                    }
                    reader.Close();
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

        protected void CalculateTimeValues(JObject result, string operationType, string statement, List<double> timeList)
        {
            double average = timeList.Sum() / _executeTimesN;
            double sumSquaredDifferences = 0;
            foreach (double time in timeList)
            {
                sumSquaredDifferences += Math.Pow(time - average, 2);
            }
            double standardDeviation = Math.Sqrt(sumSquaredDifferences / _executeTimesN);

            result[operationType][statement][_executionTimeLiteral] = Math.Round(average, 2);
            result[operationType][statement][_standardDeviationLiteral] = Math.Round(standardDeviation, 2);
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
