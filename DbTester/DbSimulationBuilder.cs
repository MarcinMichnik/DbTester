﻿using DbTester.Commands;
using DbTester.Executors;

namespace DbTester
{
    public class DbSimulationBuilder
    {
        public DbSimulation Build()
        {
            string jsonFilePath = @"..\..\..\Entries.json";
            string dbConnectionString = "Data Source=localhost;Initial Catalog=master;Integrated Security=True";
            DbSimulation simulation = new(jsonFilePath, dbConnectionString);
            AddExecutorsTo(simulation);
            return simulation;
        }

        private void AddExecutorsTo(DbSimulation simulation)
        {
            // Executor ordering matters
            InsertAllExecutor insertAllExecutor = new(simulation);
            simulation.SqlStatementExecutors.Add(insertAllExecutor);

            UpdateSingleExecutor updateSingleExecutor = new(simulation);
            simulation.SqlStatementExecutors.Add(updateSingleExecutor);

            UpdateAllExecutor updateAllExecutor = new(simulation);
            simulation.SqlStatementExecutors.Add(updateAllExecutor);

            SelectSingleExecutor selectSingleExecutor = new(simulation);
            simulation.SqlStatementExecutors.Add(selectSingleExecutor);

            SelectAllExecutor selectAllExecutor = new(simulation);
            simulation.SqlStatementExecutors.Add(selectAllExecutor);

            DeleteSingleExecutor deleteSingleExecutor = new(simulation);
            simulation.SqlStatementExecutors.Add(deleteSingleExecutor);

            InsertSingleExecutor insertSingleExecutor = new(simulation);
            simulation.SqlStatementExecutors.Add(insertSingleExecutor);
        }
    }
}
