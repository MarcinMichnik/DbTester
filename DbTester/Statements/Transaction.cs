using System.Text;

namespace QueryBuilder.Statements
{
    public sealed class Transaction
    {
        private List<IStatement> Statements { get; } = new();

        public string ToString(TimeZoneInfo timeZone)
        {
            return @$"BEGIN
                        {GetStatementLiterals(timeZone)}
                      END;";
        }

        public void AddStatement(IStatement statement)
        { 
            Statements.Add(statement);
        }

        public int GetStatementCount()
        { 
            return Statements.Count;
        }

        private string GetStatementLiterals(TimeZoneInfo timeZone)
        {
            StringBuilder transaction = new();
            foreach (IStatement statement in Statements)
            {
                string literal = statement.ToString(timeZone);
                transaction.AppendLine(literal);
            }
            return transaction.ToString();
        }
    }
}