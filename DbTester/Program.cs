using DbTester.Statements;
using Newtonsoft.Json.Linq;

string jsonFilePath = @"..\..\..\Entries.json";
string fullPath = Path.GetFullPath(jsonFilePath);
string jsonString = File.ReadAllText(fullPath);
JArray input = JArray.Parse(jsonString);

CreateTable ct = new CreateTable("T_TEST_TABLE");
string query = ct.FromJArray(input);
Console.WriteLine(query);
