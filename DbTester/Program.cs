using DbTester;

string jsonFilePath = @"..\..\..\Entries.json";
string connectionString = "Data Source=localhost;Initial Catalog=master;Integrated Security=True";
DbSimulation sim = new(jsonFilePath, connectionString);
sim.Run();