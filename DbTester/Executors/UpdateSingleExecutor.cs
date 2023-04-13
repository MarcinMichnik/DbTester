using System.Data.SqlClient;
using DbTester.Executors;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;

namespace DbTester.Commands
{
    public class UpdateSingleExecutor : AbstractExecutor, IExecutor
    {
        public UpdateSingleExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray)
        {
            TryExecuteOperation(result, "Update", "UPDATE_SINGLE", () =>
            {
                UpdateSingle(sourceArray);
            });
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
