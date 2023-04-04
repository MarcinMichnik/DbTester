namespace QueryBuilder.Statements
{
    public interface IStatement
    {
        // Serialize sql statement to string
        string ToString(TimeZoneInfo timeZone);
    }
}