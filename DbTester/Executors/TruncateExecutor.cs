using System.Data.SqlClient;
using DbTester.Statements;
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
                Truncate(result, operationType, statement);
            });
        }

        private void Truncate(JObject result, string operationType, string statement) 
        {
            Truncate truncateQuery = new(_tableName);
            SqlCommand truncateCommand = new(truncateQuery.ToString(TimeZoneInfo.Local), _connection);

            DateTime before = DateTime.Now;
            truncateCommand.ExecuteNonQuery();
            result[operationType][statement]["ExecutionTime"] = (DateTime.Now - before).TotalMilliseconds;
        }
    }
}
