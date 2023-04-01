using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbTester
{
    class CreateTable
    {
        private JArray _objects;
        private string? _query;

        public CreateTable(JArray objects)
        {
            _objects = objects;
        }

        public string GetQuery()
        {
            if (_query != null)
            {
                return _query;
            }

            StringBuilder sb = new StringBuilder();
            sb.AppendLine("CREATE TABLE T_TEST_TABLE (");

            JObject sample = (JObject)_objects.First();
            foreach (KeyValuePair<string, JToken> prop in sample)
            {
                string name = prop.Key;
                string sqlType = TokenToSqlType(prop.Value);

                sb.AppendLine($"{name} {sqlType},");
            }
            sb.Length -= 1;

            sb.AppendLine(");");

            _query = sb.ToString();

            return _query;
        }

        private string TokenToSqlType(JToken value)
        {
            Dictionary<JTokenType, string> map = new()
            {
                { JTokenType.String, "VARCHAR(64)" },
                { JTokenType.Integer, "INT" },
                { JTokenType.Float, "FLOAT(53)" },
                { JTokenType.Date, "DATETIME" }
            };
            return map[value.Type];
        }
    }
}
