using Newtonsoft.Json.Linq;
using QueryBuilder.DataTypes;

namespace QueryBuilder
{
    internal class QueryBuilderTools
    {
        public static string ConvertJTokenToString(JToken token, TimeZoneInfo timeZone)
        {
            switch (token.Type)
            {
                case JTokenType.String:
                    return JTokenTypeStringToString(token);
                case JTokenType.Integer:
                    return token.ToString();
                case JTokenType.Float:
                    return JtokenTypeFloatToString(token);
                case JTokenType.Date:
                    return JTokenTypeDateToString(token, timeZone);
                default:
                    string errorMessage = $"Cannot convert JTokenType: {token.Type} to string!";
                    throw new Exception(errorMessage);
            }
        }

        private static string JTokenTypeStringToString(JToken token)
        {
            string strToken = token.ToString();

            // If JTopkeType.String has a function call prefix,
            if (strToken.StartsWith(SqlFunction.FunctionLiteralPrefix))
                // it needs to be read without the prefix and not include single quotation marks
                return strToken[SqlFunction.FunctionLiteralPrefix.Length..];

            return $"'{strToken}'";
        }

        private static string JtokenTypeFloatToString(JToken token)
        {
            string stringLiteral = token.ToString();
            return stringLiteral.Replace(",", ".");
        }

        private static string JTokenTypeDateToString(JToken token, TimeZoneInfo timeZone)
        {
            DateTime datetimeValue = (DateTime)token;
            DateTime.SpecifyKind(datetimeValue, DateTimeKind.Unspecified);
            TimeSpan offset = timeZone.GetUtcOffset(datetimeValue);
            DateTimeOffset dto = new(datetimeValue, offset);

            string dateTimeStr = dto.ToString("yyyy-MM-dd\\\"T\\\"HH:mm:ss");
            return $"TO_DATE('{dateTimeStr}', 'YYYY-MM-DD\"T\"HH24:MI:SS')";
        }
    }
}
