using Newtonsoft.Json.Linq;

namespace DbTester.DataTypes
{
    public class Row
    {
        public Dictionary<string, JToken> Columns { get; set; }

        public Row(List<KeyValuePair<string, JToken>> columns)
        {
            Columns = new();
            foreach (KeyValuePair<string, JToken> column in columns)
            { 
                Columns.Add(column.Key, column.Value);
            }
        }
    }
}
