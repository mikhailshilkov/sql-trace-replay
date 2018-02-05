namespace SQLTraceReplayFunctionApp
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Linq;

    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Host;
    using Microsoft.Azure.WebJobs.ServiceBus;

    public static class Replay
    {
        [FunctionName("Replay")]
        public static void Run(
            [EventHubTrigger("sqltrace", Connection = "EventHubsConn")] string sql,
            TraceWriter log)
        {
            var commandName = sql
                                  .Split(null)
                                  .SkipWhile(r => r != "exec" && r != "sp_executesql")
                                  .FirstOrDefault(r => !r.Contains("exec")) ?? "<empty>";

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            try
            {
                using (var sqlConnection = new SqlConnection(AzureSqlConnectionString))
                using (var cmd = new SqlCommand())
                {
                    sqlConnection.Open();

                    cmd.CommandText = sql;
                    cmd.CommandType = CommandType.Text;

                    cmd.Connection = sqlConnection;

                    int count = 0;
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            count++;
                        }
                    }

                    log.Info($"Processed {commandName} in {stopwatch.ElapsedMilliseconds} ms with {count} rows");
                }
            }
            catch (Exception ex)
            {
                log.Error($"Error in {commandName} in {stopwatch.ElapsedMilliseconds} {ex.Message}");
                throw;
            }
        }

        private const string AzureSqlConnectionString = "<Your Azure SQL Database connection>"; // TODO: move to config
    }
}
