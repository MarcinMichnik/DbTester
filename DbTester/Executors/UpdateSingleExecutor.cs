using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;

namespace DbTester.Commands
{
    public class UpdateSingleExecutor
    {
        private readonly string _tableName;
        private readonly SqlConnection _connection;
        public UpdateSingleExecutor(string tableName, SqlConnection connection)
        { 
            _tableName = tableName;
            _connection = connection;
        }

        public void Execute(JObject result, JArray sourceArray)
        {
            TryExecuteOperation(result, "Update", "UPDATE_SINGLE", () =>
            {
                UpdateSingle(sourceArray);
            });
        }

        private void TryExecuteOperation(JObject result, string operationType, string statement, Action action)
        {
            result["TestCount"] = (int)result["TestCount"] + 1;
            result["SuccessfulTests"] = (int)result["SuccessfulTests"] + 1;
            DateTime before = DateTime.Now;
            try
            {
                action();
            }
            catch (Exception e)
            {
                AddError(result, e.Message, operationType, statement);
            }
            result[operationType][statement]["ExecutionTime"] = (DateTime.Now - before).Milliseconds;
        }

        private void AddError(JObject result, string message, string operationType, string statement)
        {
            JArray errors = (JArray)result[operationType][statement]["Errors"];
            errors.Add(message);
            result["Status"] = "Error";
            result["SuccessfulTests"] = (int)result["SuccessfulTests"] - 1;
            result["FailedTests"] = (int)result["FailedTests"] + 1;
        }

        private void UpdateSingle(JArray sourceArray)
        {
            Update updateQuery = new(_tableName);
            JProperty idProp = (JProperty)sourceArray.First().First();
            JObject firstObject = (JObject)sourceArray.First();
            foreach (JProperty? prop in firstObject.Properties())
            {
                if (prop is null)
                    continue;
                updateQuery.AddColumn(prop.Name, prop.Value);
            }
            updateQuery.Where(idProp.Name, "=", idProp.Value);

            SqlCommand updateCommand = new(updateQuery.ToString(TimeZoneInfo.Local), _connection);
            updateCommand.ExecuteNonQuery();
        }
    }
}
