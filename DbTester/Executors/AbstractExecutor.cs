using System.Data;
using System.Data.SqlClient;
using System.Text;
using DbTester.Statements;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;

namespace DbTester.Executors
{
    public abstract class AbstractExecutor
    {
        protected string _tableName;
        protected SqlConnection _connection;

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
            SqlCommand selectCommand = new(selectQuery.ToString(), _connection);

            DateTime before = DateTime.Now;
            SqlDataReader reader = selectCommand.ExecuteReader();
            while (reader.Read())
            {
                string row = ReadSingleRow(reader);
                Console.WriteLine(row); // FIXME - do not log here
            }
            reader.Close();
            result[operationType][statement]["ExecutionTime"] = (DateTime.Now - before).TotalMilliseconds;
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
