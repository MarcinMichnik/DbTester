using Newtonsoft.Json.Linq;
using System.Text;
using QueryBuilder.Statements;
using DbTester.DataTypes;

namespace DbTester.Statements
{
    public class Select : Statement, IStatement
    {
        private string _tableName;
        private bool _selectAll;

        public Select(string tableName, bool selectAllFields) 
        {
            _tableName = tableName;
            _selectAll = selectAllFields;
            WhereClauses = new();
            Rows = new();
        }

        public void AddColumn(string columnName)
        {
            Rows ??= new();

            List<KeyValuePair<string, JToken>> columns = new()
            {
                // value does not matter
                new KeyValuePair<string, JToken>(columnName, 0)
            };
            Row row = new(columns);
            Rows.Add(row);
        }

        public override string ToString()
        {
            return ToString(TimeZoneInfo.Local);
        }

        public string ToString(TimeZoneInfo info)
        {
            string columnNames = SerializeColumnNames();
            if (WhereClauses is null || WhereClauses.Count() == 0)
            { 
                return $"SELECT {columnNames} FROM {_tableName};";
            }
            string whereClauses = SerializeWhereClauses(info);
            return $"SELECT {columnNames} FROM {_tableName} WHERE {whereClauses};";
        }

        private string SerializeColumnNames()
        {
            if (_selectAll)
                return "*";

            StringBuilder columns = new();

            Rows ??= new();
            Row first = Rows.First();
            foreach (KeyValuePair<string, JToken> column in first.Columns)
            {
                string columnLiteral = $"{column.Key},";
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
