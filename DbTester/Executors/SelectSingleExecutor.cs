using DbTester.Statements;
using Newtonsoft.Json.Linq;

namespace DbTester.Executors
{
    public class SelectSingleExecutor : AbstractExecutor, IExecutor
    {
        public SelectSingleExecutor(DbSimulation simulation) : base(simulation) { }
        public void Execute(JObject result, JArray sourceArray)
        {
            string operationType = "Read";
            string statement = "SELECT_SINGLE";
            TryExecuteOperation(result, operationType, statement, () =>
            {
                SelectAndReadSingle(result, sourceArray, operationType, statement);
            });
        }

        private void SelectAndReadSingle(JObject result, JArray sourceArray, string operationType, string statement)
        {
            Select selectQuery = new(_tableName, selectAllFields: true);
            JProperty id = (JProperty)sourceArray.First().First();
            selectQuery.Where(id.Name, "=", id.Value);
            SelectAndRead(result, selectQuery, operationType, statement);
        }
    }
}
