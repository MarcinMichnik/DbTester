using System.Text;
using DbTester.DataTypes;
using Newtonsoft.Json.Linq;

namespace QueryBuilder.Statements
{
    public abstract class Statement : AbstractBase
    {
        // Dict where key is FilteredColumnName,
        // value is a pair where key is an arithmetic operator sign
        // and value is used on the right side of the where clause

        // By default where clauses are not available; To be created in constructor
        protected Dictionary<string, KeyValuePair<string, JToken>>? WhereClauses { get; set; } = null;
        public List<Row> Rows { get; set; } = new();

        public void AddRow(Row row)
        {
            Rows.Add(row);
        }

        public void Where(string columnName, string arithmeticSign, JToken value)
        {
            WhereClauses ??= new();

            KeyValuePair<string, JToken> pair = new(arithmeticSign, value);
            WhereClauses.Add(columnName, pair);
        }

        protected string SerializeWhereClauses(TimeZoneInfo timeZone)
        {
            if (WhereClauses is null || WhereClauses.Count == 0)
                return string.Empty;

            StringBuilder whereClauseLiterals = new();

            AppendWhereClauseLiterals(timeZone, whereClauseLiterals);

            return whereClauseLiterals.ToString();
        }

        private void AppendWhereClauseLiterals(TimeZoneInfo timeZone, StringBuilder whereClauseLiterals)
        {
            if (WhereClauses is null)
                throw new Exception("Cannot serialize where clause to string because WhereClauses property is null!");

            foreach (KeyValuePair<string, KeyValuePair<string, JToken>> primaryKeyLookup in WhereClauses)
            {
                string arithmeticSign = primaryKeyLookup.Value.Key;
                string convertedValue = QueryBuilderTools.ConvertJTokenToString(primaryKeyLookup.Value.Value, timeZone);
                string whereClauseLiteral = $"{primaryKeyLookup.Key} {arithmeticSign} {convertedValue} AND ";
                whereClauseLiterals.Append(whereClauseLiteral);
            }

            whereClauseLiterals.Length -= 5; // remove last " AND "
        }
    }
}
