using DbTester;

string jsonFilePath = @"..\..\..\Entries.json";
string dbConnectionString = "Data Source=localhost;Initial Catalog=master;Integrated Security=True";
DbSimulation simulation = new(jsonFilePath, dbConnectionString, silent: true);
string result = simulation.Run();
Console.WriteLine(result);