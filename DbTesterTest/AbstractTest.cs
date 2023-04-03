using QueryBuilder.DataTypes;
using QueryBuilder.Statements;

namespace QueryBuilderTest
{
    public abstract class AbstractTest
    {
        protected SqlFunction CurrentTimestampCall { get; set; } = new("CURRENT_TIMESTAMP()");
        protected string ModifiedBy { get; set; } = "XYZ";
        protected string TableName { get; set; } = "\"APP\".\"EXAMPLE_TABLE_NAME\"";
        protected TimeZoneInfo TimeZone { get; set; }
            = TimeZoneInfo.FindSystemTimeZoneById("Central European Standard Time");

        protected Insert GetInsertWithMasterPrimaryKey(int id)
        {
            Insert query = new(TableName);

            query.AddColumn("MASTER_ID", new SqlFunction("SEQ.NEXT_VAL"));
            query.AddColumn("ID", id);
            query.AddColumn("NAME", "HANNAH");
            query.AddColumn("SAVINGS", 12.1);
            query.AddColumn("MODIFIED_AT", CurrentTimestampCall);
            query.AddColumn("MODIFIED_BY", ModifiedBy);

            return query;
        }

        protected Update GetUpdateWithManyPrimaryKeys()
        {
            Update query = new(TableName);
            query.AddColumn("NAME", "HANNAH");
            query.AddColumn("SAVINGS", 12.1);
            query.AddColumn("MODIFIED_AT", CurrentTimestampCall);
            query.AddColumn("MODIFIED_BY", ModifiedBy);

            query.Where("ID", "=", 1);
            query.Where("EXTERNAL_ID", "=", 301);

            return query;
        }
    }
}
