using Newtonsoft.Json.Linq;
using System.Text;

namespace DbTester
{
    class DatabaseSetupManager
    {
        private JArray _objects;
        private string _query;

        public DatabaseSetupManager(JArray objects) 
        {
            _objects = objects;
            _query = GetSetupQuery();
        }

        public string GetSetupQuery() {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("CREATE TABLE T_TEST_TABLE (");

            JObject sample = (JObject)_objects.First();
            foreach (KeyValuePair<string, JToken> prop in sample)
            {
                sb.AppendLine("");
            }

            return sb.ToString();
        }
    }
}
