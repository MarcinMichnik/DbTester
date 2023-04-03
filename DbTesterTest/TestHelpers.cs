using System.Text.RegularExpressions;

namespace QueryBuilderTest
{
    static class TestHelpers
    {
        public static TimeZoneInfo TimeZone { get; set; }
            = TimeZoneInfo.FindSystemTimeZoneById("Central European Standard Time");

        public static string RemoveWhitespace(string str)
        {
            return Regex.Replace(str, @"\s", "");
        }
    }
}
