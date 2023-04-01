// See https://aka.ms/new-console-template for more information
using DbTester;
using Newtonsoft.Json.Linq;

JArray sampleInput = JArray.Parse("[ \r\n\r\n    { \r\n\r\n        \"Id\": 1, \r\n\r\n        \"Name\": \"Adam\", \r\n\r\n        \"Salary\": 1256.34, \r\n\r\n        \"DateOfBirth\": \"1996-05-24T08:21:23\" \r\n\r\n    }, \r\n\r\n    { \r\n\r\n        \"Id\": 2, \r\n\r\n        \"Name\": \"Marcus\", \r\n\r\n        \"Salary\": 1321.89, \r\n\r\n        \"DateOfBirth\": \"1993-03-02T13:45:12\" \r\n\r\n    }, \r\n\r\n    { \r\n\r\n        \"Id\": 3, \r\n\r\n        \"Name\": \"Jack\", \r\n\r\n        \"Salary\": 924.67, \r\n\r\n        \"DateOfBirth\": \"1994-02-04T12:01:13\" \r\n\r\n    } \r\n\r\n] ");
CreateTable ct = new CreateTable(sampleInput);
string query = ct.GetQuery();
Console.WriteLine(query);
