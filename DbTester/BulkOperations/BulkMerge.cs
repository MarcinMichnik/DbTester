using System.Text;
using DbTester.DataTypes;
using Newtonsoft.Json.Linq;
using QueryBuilder.DataTypes;
using QueryBuilder.Statements;

namespace QueryBuilder.BulkOperations
{
    // Can produce many sql transactions in case there are too many statements
    public sealed class BulkMerge : AbstractBase
    {
        private readonly JArray incomingEntities;
        private readonly JArray existingTableState;
        private readonly List<string> primaryKeyIdentifiers;

        private readonly List<Transaction> transactions = new();
        private readonly Dictionary<OperationResult, int> operationResults = new() 
        {
            { OperationResult.INSERTED, 0 },
            { OperationResult.UPDATED, 0 },
            { OperationResult.SKIPPED, 0 }
        };
        public ushort MaxTransactionSize { get; } = 2048;

        public BulkMerge(
            JArray incomingEntities,
            JArray existingTableState,
            string tableName,
            List<string> primaryKeyIdentifiers)
        {
            this.tableName = tableName;
            this.incomingEntities = incomingEntities;
            this.existingTableState = existingTableState;
            this.primaryKeyIdentifiers = primaryKeyIdentifiers;

            InitializeTransactions();
        }

        private void InitializeTransactions()
        {
            Transaction transaction = new();

            for (int i = 0; i < incomingEntities.Count; i++)
            {
                JToken entity = incomingEntities[i];
                IEnumerable<JToken> matches = FindMatches(entity);
                IStatement? statement = TryGetStatement(entity, matches);
                AddUpOperationResult(statement);

                if (statement != null)
                    transaction.AddStatement(statement);

                if (transaction.GetStatementCount() % MaxTransactionSize == 0)
                {
                    transactions.Add(transaction);
                    transaction = new Transaction();
                }

                if (IsLastLoopIteration(i))
                    transactions.Add(transaction);
            }
        }

        private void AddUpOperationResult(IStatement? statement)
        {
            if (statement == null)
            {
                operationResults[OperationResult.SKIPPED]++;
                return;
            }

            if (statement.GetType() == typeof(Insert))
            {
                operationResults[OperationResult.INSERTED]++;
                return;
            }
            else { // Update
                operationResults[OperationResult.UPDATED]++;
                return;
            }
        }

        private bool IsLastLoopIteration(int i)
        {
            return i == incomingEntities.Count - 1;
        }

        public void AddTransaction(Transaction transaction)
        {
            transactions.Add(transaction);
        }

        public int GetTransactionCount()
        { 
            return transactions.Count;
        }

        private IEnumerable<JToken> FindMatches(JToken original)
        {
            IEnumerable<JToken> matches = existingTableState;

            if (primaryKeyIdentifiers.Count == 0)
                return Enumerable.Empty<JToken>();

            foreach (string identifier in primaryKeyIdentifiers)
            {
                matches = matches.Where(
                    x => x[identifier].ToString() == original[identifier].ToString());
            }

            return matches;
        }

        private IStatement? TryGetStatement(JToken entity, IEnumerable<JToken> matches)
        {
            if (!matches.Any())
                return CreateInsertStatement(entity);

            JToken match = matches.First();
            if (!JToken.DeepEquals(entity, match))
                return CreateUpdateStatement(entity);

            return null;
        }
        private Insert CreateInsertStatement(JToken token)
        {
            Insert insert = new(tableName, token);

            Row first = insert.Rows.First();
            List<KeyValuePair<string, JToken>> columns = new();
            SqlFunction func = new("CURRENT_TIMESTAMP");
            first.Columns.Add("MODIFIED_AT", func.GetPrefixedLiteral());
            first.Columns.Add("MODIFIED_BY", ModifiedBy);
            Row row = new(columns);

            insert.AddRow(row);
            return insert;
        }

        private Update CreateUpdateStatement(JToken token)
        {
            Update update = new(tableName);
            JObject tokenObject = (JObject)token;
            List<KeyValuePair<string, JToken>> columns = new();
            foreach (JProperty prop in tokenObject.Properties())
            {
                if (IsNotPrimaryKey(prop.Name))
                {
                    columns.Add(new KeyValuePair<string, JToken>(prop.Name, prop.Value));
                }
            }

            string literal = CurrentTimestampCall.GetPrefixedLiteral();
            columns.Add(new KeyValuePair<string, JToken>("MODIFIED_AT", literal));
            columns.Add(new KeyValuePair<string, JToken>("MODIFIED_BY", ModifiedBy));

            Row row = new(columns);
            update.AddRow(row);

            foreach (string identifier in primaryKeyIdentifiers)
            {
                update.Where(identifier, "=", token[identifier]);
            }

            return update;
        }

        private bool IsNotPrimaryKey(string columnName)
        {
            return !primaryKeyIdentifiers.Contains(columnName);
        }

        public string ToString(TimeZoneInfo timeZone)
        {
            StringBuilder text = new();
            foreach (Transaction t in transactions)
            {
                string transactionStr = t.ToString(timeZone);
                text.AppendLine(transactionStr);
            }
            return text.ToString();
        }

        public int GetInsertCount()
        {
            return operationResults[OperationResult.INSERTED];
        }

        public int GetUpdateCount()
        {
            return operationResults[OperationResult.UPDATED];
        }

        public int GetSkipCount()
        {
            return operationResults[OperationResult.SKIPPED];
        }
    }
}
