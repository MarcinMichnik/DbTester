using System.Text;
using Newtonsoft.Json.Linq;

namespace QueryBuilder.Statements
{
    public sealed class Insert : Statement, IStatement
    {
        public Insert(string tableName)
        {
            this.tableName = tableName;
            Columns = new();
        }

        public Insert(string tableName, JToken token)
        {
            this.tableName = tableName;
            Columns = new();
            JObject tokenObject = (JObject)token;
            foreach (JProperty prop in tokenObject.Properties())
            {
                AddColumn(prop.Name, prop.Value);
            }
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
            if (Columns == null)
                throw new Exception("Cannot get update columns because Columns property is null!");

            StringBuilder columnStringBuilder = new();

            foreach (KeyValuePair<string, JToken> column in Columns)
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
            if (Columns == null)
                throw new Exception("Cannot get update values because Columns property is null!");

            StringBuilder columnStringBuilder = new();

            foreach (KeyValuePair<string, JToken> column in Columns)
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