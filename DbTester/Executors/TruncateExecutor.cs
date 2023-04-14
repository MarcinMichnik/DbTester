using Newtonsoft.Json.Linq;

namespace DbTester.Executors
{
    public class TruncateExecutor : AbstractExecutor, IExecutor
    {
        public TruncateExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray)
        {
            string operationType = "Delete";
            string statement = "TRUNCATE";

            TryExecuteOperation(result, operationType, statement, () => 
            {
                Truncate(result, sourceArray, operationType, statement);
            });
        }

        private void Truncate(JObject result, JArray sourceArray, string operationType, string statement) 
        {
            
        }
    }
}
