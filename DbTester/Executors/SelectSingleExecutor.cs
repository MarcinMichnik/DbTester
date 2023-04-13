using DbTester.Statements;
using System.Data.SqlClient;
using Newtonsoft.Json.Linq;

namespace DbTester.Executors
{
    public class SelectSingleExecutor : AbstractExecutor, IExecutor
    {
        public SelectSingleExecutor(DbSimulation simulation) : base(simulation) { }
        public void Execute(JObject result, JArray sourceArray)
        {
            TryExecuteOperation(result, "Read", "SELECT_SINGLE", () =>
            {
                SelectAndReadSingle(sourceArray);
            });
        }

        private void SelectAndReadSingle(JArray sourceArray)
        {
            Select selectQuery = new(_tableName, selectAllFields: true);
            JProperty id = (JProperty)sourceArray.First().First();
            selectQuery.Where(id.Name, "=", id.Value);
            SelectAndRead(selectQuery);
        }
    }
}
