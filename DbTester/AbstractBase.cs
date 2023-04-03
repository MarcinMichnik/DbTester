using Newtonsoft.Json.Linq;
using QueryBuilder.DataTypes;

namespace QueryBuilder
{
    public abstract class AbstractBase
    {
        protected string? tableName;
        protected string ModifiedBy { get; set; } = "XYZ";
        protected SqlFunction CurrentTimestampCall { get; set; } = new("CURRENT_TIMESTAMP()");
    }
}
