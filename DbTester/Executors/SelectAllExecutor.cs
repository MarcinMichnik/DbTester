using DbTester.Statements;
using Newtonsoft.Json.Linq;

namespace DbTester.Executors
{
    public class SelectAllExecutor : AbstractExecutor, IExecutor
    {
        public SelectAllExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray)
        {
            TryExecuteOperation(result, "Read", "SELECT_ALL", () =>
            {
                SelectAndReadAll();
            });
        }

        private void SelectAndReadAll()
        {
            Select selectQuery = new(_tableName, selectAllFields: true);
            SelectAndRead(selectQuery);
        }
    }
}
