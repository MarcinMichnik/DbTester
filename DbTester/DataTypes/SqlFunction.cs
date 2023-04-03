namespace QueryBuilder.DataTypes
{
    public sealed class SqlFunction
    {
        // used to distinguish sql function calls from regular string values
        // since both are stored as a JTokenType.String
        // sql function calls will start with this prefix
        // IMPORTANT: this implementation presumes
        // that "\f\n" would never be used as first characters in a function literal name
        public static string FunctionLiteralPrefix { get; } = "\f\n";
        public string Literal { get; }

        public SqlFunction(string literal)
        {
            Literal = literal;
        }

        public string GetPrefixedLiteral()
        {
            return $"{FunctionLiteralPrefix}{Literal}";
        }
    }
}
