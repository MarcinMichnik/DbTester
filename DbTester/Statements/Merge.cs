using QueryBuilder;
using QueryBuilder.Statements;

namespace DbTester.Statements
{
    public class Merge : Statement, IStatement
    {
        public Merge(string tableName)
        {
            this.tableName = tableName;
        }

        public string ToString(TimeZoneInfo timeZoneInfo)
        {
            string[] columnNames = Rows.First().Columns.Select(c => c.Key).ToArray(); // Assumes there is always at least one row in Rows.

            var rowsString = string.Join(", ", Rows.Select(row =>
            {
                string[] columnValues = row.Columns.Select(c => QueryBuilderTools.ConvertJTokenToString(c.Value, timeZoneInfo)).ToArray();
                return $"({string.Join(", ", columnValues)})";
            }));

            return $@"MERGE INTO {tableName} AS T
              USING (VALUES {rowsString}) AS S ({string.Join(", ", columnNames)})
              ON T.ID = S.ID
              WHEN MATCHED THEN
                  UPDATE SET {string.Join(", ", columnNames.Select((c, i) => $"T.{c} = S.{c}"))}
              WHEN NOT MATCHED BY TARGET THEN
                  INSERT ({string.Join(", ", columnNames)})
                  VALUES ({string.Join(", ", columnNames.Select(c => $"S.{c}"))})
              WHEN NOT MATCHED BY SOURCE THEN
                  DELETE;";
        }
    }
}
