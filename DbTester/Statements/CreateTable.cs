using Newtonsoft.Json.Linq;
using QueryBuilder;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbTester.Statements
{
    public class CreateTable
    {
        private string? _tableName;
        private JArray _objects;

        public CreateTable(string tableName, JArray objects)
        {
            _tableName = tableName;
            _objects = objects;
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"CREATE TABLE {_tableName} (");

            JObject sample = (JObject)_objects.First();
            List<JProperty> props = sample.Properties().ToList();
            for (int i = 0; i < sample.Count; i++)
            {
                JProperty prop = props[i];
                string name = prop.Name;
                string sqlType = QueryBuilderTools.TokenToSqlType(prop.Value);

                sb.Append($"    {name} {sqlType}");
                if (i != sample.Count - 1)
                {
                    sb.AppendLine(",");
                }
                else
                {
                    sb.Append('\n');
                }
            }

            sb.AppendLine(");");

            return sb.ToString();
        }
    }
}
