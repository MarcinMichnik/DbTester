using QueryBuilder.Statements;

namespace DbTester.Statements
{
    public class Truncate : Statement, IStatement
    {
        public Truncate(string tableName)
        {
            this.tableName = tableName;
        }

        public string ToString(TimeZoneInfo timeZone)
        {
            return $@"TRUNCATE TABLE {tableName};";
        }
    }
}
