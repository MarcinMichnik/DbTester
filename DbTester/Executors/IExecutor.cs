using Newtonsoft.Json.Linq;

namespace DbTester.Executors
{
    public interface IExecutor
    {
        void Execute(JObject result, JArray sourceArray);
    }
}
