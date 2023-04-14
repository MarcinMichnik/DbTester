using System.Text;

namespace QueryBuilder.Statements
{
    public sealed class Delete : Statement, IStatement
    {
        public Delete(string tableName)
        {
            this.tableName = tableName;
        }

        public string ToString(TimeZoneInfo timeZone)
        {
            StringBuilder sb = new();
            sb.Append(@$"DELETE FROM {tableName}");
            if (WhereClauses != null)
            {
                string whereClauseLiterals = SerializeWhereClauses(timeZone);
                sb.Append($" WHERE {whereClauseLiterals}");
            }
            sb.Append(';');
            return sb.ToString();
        }
    }
}