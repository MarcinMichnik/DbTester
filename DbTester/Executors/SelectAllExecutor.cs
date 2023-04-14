using DbTester.Statements;
using Newtonsoft.Json.Linq;

namespace DbTester.Executors
{
    public class SelectAllExecutor : AbstractExecutor, IExecutor
    {
        public SelectAllExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray)
        {
            string operationType = "Read";
            string statement = "SELECT_ALL";
            TryExecuteOperation(result, operationType, statement, () =>
            {
                SelectAndReadAll(result, operationType, statement);
            });
        }

        private void SelectAndReadAll(JObject result, string operationType, string statement)
        {
            Select selectQuery = new(_tableName, selectAllFields: true);
            SelectAndRead(result, selectQuery, operationType, statement);
        }
    }
}
