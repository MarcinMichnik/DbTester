using System.Text;
using DbTester.DataTypes;
using Newtonsoft.Json.Linq;

namespace QueryBuilder.Statements
{
    public sealed class Update : Statement, IStatement
    { 
        public Update(string tableName)
        {
            this.tableName = tableName;
            WhereClauses = new();
        }

        public string ToString(TimeZoneInfo timeZone)
        {
            if (WhereClauses is null || WhereClauses.Count == 0)
            {
                string errorMessage = 
                    @"Cannot serialize update object to string 
                      because WhereClauses property is null or contains zero elements!";
                throw new Exception(errorMessage);
            }

            string primaryKeyLookups = SerializeWhereClauses(timeZone);
            string columns = SerializeColumns(timeZone);

            return @$"UPDATE {tableName} SET
                          {columns} 
                      WHERE {primaryKeyLookups};";
        }

        private string SerializeColumns(TimeZoneInfo timeZone)
        {
            StringBuilder columns = new();

            Row first = Rows.First();
            foreach (KeyValuePair<string, JToken> column in first.Columns)
            {
                string convertedValue = QueryBuilderTools.ConvertJTokenToString(column.Value, timeZone);
                string columnLiteral = $"{column.Key} = {convertedValue},";
                columns.AppendLine(columnLiteral);
            }

            // remove last comma and newline
            int newLineLength = Environment.NewLine.Length;
            int valueToSubtract = newLineLength + 1;
            columns.Length -= valueToSubtract;

            return columns.ToString();
        }
    }
}