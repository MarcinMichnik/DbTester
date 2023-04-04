namespace DbTester.Statements
{
    public class DropTable
    {
        private readonly string _tableName;
        public DropTable(string tableName) 
        {
            _tableName = tableName;
        }

        public override string ToString()
        {
            return $"DROP TABLE {_tableName};";
        }
    }
}
