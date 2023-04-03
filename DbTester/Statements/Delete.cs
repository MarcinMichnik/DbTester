namespace QueryBuilder.Statements
{
    public sealed class Delete : Statement, IStatement
    {
        public Delete(string tableName)
        {
            this.tableName = tableName;
            WhereClauses = new();
        }

        public string ToString(TimeZoneInfo timeZone)
        {
            string whereClauseLiterals = SerializeWhereClauses(timeZone);

            return @$"DELETE FROM {tableName} 
                      {whereClauseLiterals};";
        }
    }
}