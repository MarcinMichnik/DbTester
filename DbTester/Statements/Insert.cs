using System.Text;
using DbTester.DataTypes;
using Newtonsoft.Json.Linq;

namespace QueryBuilder.Statements
{
    public sealed class Insert : Statement, IStatement
    {
        public Insert(string tableName)
        {
            this.tableName = tableName;
            Rows = new();
        }

        public Insert(string tableName, JToken token)
        {
            this.tableName = tableName;
            Rows = new();
            JObject tokenObject = (JObject)token;

            List<KeyValuePair<string, JToken>> columns = new();
            foreach (JProperty prop in tokenObject.Properties())
            {
                KeyValuePair<string, JToken> pair = new(prop.Name, prop.Value);
                columns.Add(pair);
            }
            Row row = new(columns);
            Rows.Add(row);
        }

        public string ToString(TimeZoneInfo timeZone)
        {
            string columns = SerializeColumns();
            string values = SerializeValues(timeZone);

            return @$"INSERT INTO {tableName} (
                          {columns}
                      ) VALUES (
                          {values}
                      );";
        }

        private string SerializeColumns()
        {
            StringBuilder columnStringBuilder = new();
            Rows ??= new();
            Row first = Rows.First(); // Insert always has one element
            foreach (KeyValuePair<string, JToken> column in first.Columns)
            {
                string columnLiteral = $"{column.Key},";
                columnStringBuilder.AppendLine(columnLiteral);
            }

            RemoveTrailingSigns(columnStringBuilder);

            return columnStringBuilder.ToString();
        }

        private static void RemoveTrailingSigns(StringBuilder columns)
        {
            int newLineStrLength = Environment.NewLine.Length;
            int newLength = columns.Length - newLineStrLength - 1;
            columns.Length = newLength;
        }

        private string SerializeValues(TimeZoneInfo timeZone)
        {
            StringBuilder columnStringBuilder = new();
            Rows ??= new();
            Row first = Rows.First(); // Insert always has one element
            foreach (KeyValuePair<string, JToken> column in first.Columns)
            {
                string convertedValue = QueryBuilderTools.ConvertJTokenToString(column.Value, timeZone);
                string columnLiteral = $"{convertedValue},";
                columnStringBuilder.AppendLine(columnLiteral);
            }

            RemoveTrailingSigns(columnStringBuilder);

            return columnStringBuilder.ToString();
        }
    }
}