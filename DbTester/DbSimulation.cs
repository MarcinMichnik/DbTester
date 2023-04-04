﻿using System.Data.SqlClient;
using System.Data;
using DbTester.Statements;
using Newtonsoft.Json.Linq;
using QueryBuilder.Statements;
using QueryBuilder.DataTypes;

namespace DbTester
{
    public class DbSimulation
    {
        private string _filePath;
        private string _dbConnectionString;
        private readonly string _tableName = "T_TEST_TABLE";
        public DbSimulation(string filePath, string dbConnectionString)
        {
            _filePath = filePath;
            _dbConnectionString = dbConnectionString;
        }
        public string Run()
        {
            string fullPath = Path.GetFullPath(_filePath);
            string jsonString = File.ReadAllText(fullPath);
            JArray input = JArray.Parse(jsonString);
            CreateTable ct = new(_tableName, input);
            string createTableQuery = ct.ToString();

            SqlConnection connection = new(_dbConnectionString);
            connection.Open();
            SqlCommand createTableCommand = new(createTableQuery, connection);
            try 
            {
                createTableCommand.ExecuteNonQuery();
            } catch (Exception ex) 
            {
                DropTable dropTableQueryExc = new(_tableName);
                SqlCommand dropTableCommandExc = new(dropTableQueryExc.ToString(), connection);
                dropTableCommandExc.ExecuteNonQuery();
                createTableCommand.ExecuteNonQuery();
            }
            
            foreach (JObject obj in input.Children<JObject>())
            {
                Insert insertQuery = new(_tableName);
                foreach (JProperty prop in obj.Properties())
                {
                    string propName = prop.Name;
                    JToken val = prop.Value;
                    insertQuery.AddColumn(propName, val);
                }
                SqlCommand insertCommand = new(insertQuery.ToString(TimeZoneInfo.Local), connection);
                insertCommand.ExecuteNonQuery();
            }    

            Select selectQuery = new(_tableName, true);
            SqlCommand selectCommand = new(selectQuery.ToString(), connection);
            SqlDataReader reader = selectCommand.ExecuteReader();

            while (reader.Read())
            {
                ReadSingleRow(reader);
            }
            reader.Close();

            DropTable dropTableQuery = new(_tableName);
            SqlCommand dropTableCommand = new(dropTableQuery.ToString(), connection);
            dropTableCommand.ExecuteNonQuery();

            connection.Close();
            return new JArray().ToString();
        }

        private void ReadSingleRow(IDataRecord dataRecord)
        {
            for (int i = 0; i < dataRecord.FieldCount; i++)
            {
                Console.Write(dataRecord[i]);
                if (i != dataRecord.FieldCount - 1)
                {
                    Console.Write(", ");
                }
            }
            Console.Write('\n');
        }
    }
}
