using DbTester;

DbSimulation simulation = new DbSimulationBuilder().Build();
string result = simulation.Run();
Console.WriteLine(result);

string outputFilePath = @"..\..\..\Output.json";
File.WriteAllText(outputFilePath, result);