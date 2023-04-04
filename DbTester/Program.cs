using DbTester;
using DbTester.Statements;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Data.SqlClient;

string jsonFilePath = @"..\..\..\Entries.json";
var sim = new DbSimulation(jsonFilePath);
sim.Run();