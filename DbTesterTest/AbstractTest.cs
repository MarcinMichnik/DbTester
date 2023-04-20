using DbTester.DataTypes;
using Newtonsoft.Json.Linq;
using QueryBuilder.DataTypes;
using QueryBuilder.Statements;

namespace QueryBuilderTest
{
    public abstract class AbstractTest
    {
        protected SqlFunction CurrentTimestampCall { get; set; } = new("CURRENT_TIMESTAMP");
        protected string ModifiedBy { get; set; } = "XYZ";
        protected string TableName { get; set; } = "\"APP\".\"EXAMPLE_TABLE_NAME\"";
        protected TimeZoneInfo TimeZone { get; set; }
            = TimeZoneInfo.FindSystemTimeZoneById("Central European Standard Time");

        protected Insert GetInsertWithMasterPrimaryKey(int id)
        {
            Insert query = new(TableName);
            List<KeyValuePair<string, JToken>> columns = new();
            SqlFunction func = new("SEQ.NEXT_VAL");
            string funcLiteral = func.GetPrefixedLiteral();
            columns.Add(new KeyValuePair<string, JToken>("MASTER_ID", funcLiteral));
            columns.Add(new KeyValuePair<string, JToken>("ID", id));
            columns.Add(new KeyValuePair<string, JToken>("NAME", "HANNAH"));
            columns.Add(new KeyValuePair<string, JToken>("SAVINGS", 12.1));
            string timestampLiteral = CurrentTimestampCall.GetPrefixedLiteral();
            columns.Add(new KeyValuePair<string, JToken>("MODIFIED_AT", timestampLiteral));
            columns.Add(new KeyValuePair<string, JToken>("MODIFIED_BY", ModifiedBy));
            Row row = new(columns);
            query.AddRow(row);

            return query;
        }

        protected Update GetUpdateWithManyPrimaryKeys()
        {
            Update query = new(TableName);

            List<KeyValuePair<string, JToken>> columns = new();
            columns.Add(new KeyValuePair<string, JToken>("NAME", "HANNAH"));
            columns.Add(new KeyValuePair<string, JToken>("SAVINGS", 12.1));
            string timestampLiteral = CurrentTimestampCall.GetPrefixedLiteral();
            columns.Add(new KeyValuePair<string, JToken>("MODIFIED_AT", timestampLiteral));
            columns.Add(new KeyValuePair<string, JToken>("MODIFIED_BY", ModifiedBy));
            Row row = new(columns);
            query.AddRow(row);

            query.Where("ID", "=", 1);
            query.Where("EXTERNAL_ID", "=", 301);

            return query;
        }
    }
}
