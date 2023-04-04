using Newtonsoft.Json.Linq;
using QueryBuilder;
using System.Text;
using QueryBuilder.Statements;

namespace DbTester.Statements
{
    public class Select : Statement, IStatement
    {
        private string _tableName;
        private bool _selectAll;

        public Select(string tableName, bool selectAll) 
        {
            _tableName = tableName;
            _selectAll = selectAll;
            WhereClauses = new();
            Columns = new();
        }

        public void AddColumn(string columnName)
        {
            if (Columns is null)
            {
                throw new Exception(
                    "Cannot serialize update columns because Columns property is null!");
            }
                
            // value does not matter
            Columns.Add(columnName, 0);
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
            if (Columns is null)
                throw new Exception(
                    "Cannot serialize update columns because Columns property is null!");

            StringBuilder columns = new();

            foreach (KeyValuePair<string, JToken> column in Columns)
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
