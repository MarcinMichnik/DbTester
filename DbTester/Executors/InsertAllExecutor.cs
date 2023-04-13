using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;

namespace DbTester.Executors
{
    public class InsertAllExecutor : AbstractExecutor, IExecutor
    {
        public InsertAllExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray)
        {
            TryExecuteOperation(result, "Create", "INSERT_ALL", () =>
            {
                InsertEach(sourceArray);
            });
        }

        private void InsertEach(JArray sourceArray)
        {
            foreach (JObject obj in sourceArray.Children<JObject>())
            {
                Insert insertQuery = new(_tableName);
                foreach (JProperty prop in obj.Properties())
                {
                    string propName = prop.Name;
                    JToken val = prop.Value;
                    insertQuery.AddColumn(propName, val);
                }
                SqlCommand insertCommand = new(insertQuery.ToString(TimeZoneInfo.Local), _connection);
                insertCommand.ExecuteNonQuery();
            }
        }
    }
}
