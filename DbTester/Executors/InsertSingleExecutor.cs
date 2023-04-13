﻿using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;

namespace DbTester.Executors
{
    public class InsertSingleExecutor : AbstractExecutor, IExecutor
    {
        public InsertSingleExecutor(DbSimulation simulation) : base(simulation) { }

        public void Execute(JObject result, JArray sourceArray)
        {
            string operationType = "Create";
            string statement = "INSERT_SINGLE";
            TryExecuteOperation(result, operationType, statement, () =>
            {
                InsertSingle(result, sourceArray, operationType, statement);
            });
        }

        private void InsertSingle(JObject result, JArray sourceArray, string operationType, string statement) 
        {
            Insert insertQuery = new(_tableName);

            JObject first = (JObject)sourceArray.First();
            foreach (JProperty prop in first.Properties())
            {
                string propName = prop.Name;
                JToken val = prop.Value;
                insertQuery.AddColumn(propName, val);
            }
            SqlCommand insertCommand = new(insertQuery.ToString(TimeZoneInfo.Local), _connection);

            DateTime before = DateTime.Now;
            insertCommand.ExecuteNonQuery();
            result[operationType][statement]["ExecutionTime"] = (DateTime.Now - before).TotalMilliseconds;
        }
    }
}
