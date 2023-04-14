using QueryBuilder.Statements;

namespace DbTester.Statements
{
    public class Merge : Statement, IStatement
    {
        public Merge(string tableName)
        {
            this.tableName = tableName;
        }

        public string ToString(TimeZoneInfo timeZone)
        {
            return string.Empty;
        }
    }
}
