namespace SQLTraceCapture
{
    using System;
    using System.Collections.Concurrent;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.EventHubs;
    using Microsoft.SqlServer.Management.Common;
    using Microsoft.SqlServer.Management.Trace;

    class Program
    {
        static void Main(string[] args)
        {
            // 1. Run trace server
            var connectionInfo = new SqlConnectionInfo(args[0])
            {
                DatabaseName = args[1],
                UseIntegratedSecurity = true
            };
            var trace = new TraceServer();
            trace.InitializeAsReader(connectionInfo, args[2]);

            // 2. Continuously read trace and send them to event hubs
            var tokenSource = new CancellationTokenSource();
            var readerTask = Task.Factory.StartNew(() => ReadTrace(trace, tokenSource.Token), tokenSource.Token);
            var senderTask = Task.Factory.StartNew(() => SendToEventHubs(tokenSource.Token), tokenSource.Token);

            // 3. Stop the trace
            Console.WriteLine("Press any key to stop...");
            Console.ReadKey();
            Console.WriteLine("Stopping....");
            tokenSource.Cancel();

            Task.WaitAll(readerTask, senderTask);
            Console.WriteLine("Stopped");
        }

        private static void ReadTrace(TraceServer trace, CancellationToken token)
        {
            while (trace.Read() && !token.IsCancellationRequested)
            {
                var eventClass = trace["EventClass"].ToString();
                if (string.Compare(eventClass, "RPC:Completed") == 0)
                {
                    var textData = trace["TextData"].ToString();
                    if (!textData.Contains("sp_reset_connection")
                        && !textData.Contains("sp_trace")
                        && !textData.Contains("sqlagent"))
                    {
                        eventQueue.Enqueue(textData);
                    }
                }
            }

            trace.Stop();
            trace.Close();
        }

        private static void SendToEventHubs(CancellationToken token)
        {
            var client = EventHubClient.CreateFromConnectionString(EventHubsConnectionString);
            var batch = client.CreateBatch();
            while (!token.IsCancellationRequested)
            {
                if (!eventQueue.TryDequeue(out string sql))
                {
                    Thread.Sleep(10);
                    continue;
                }

                var eventData = new EventData(Encoding.UTF8.GetBytes(sql));
                if (!batch.TryAdd(eventData) && batch.Count > 0)
                {
                    client.SendAsync(batch.ToEnumerable())
                        .ContinueWith(OnAsyncMethodFailed, token, TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);
                    batch = client.CreateBatch();
                    batch.TryAdd(eventData);
                }
            }
        }

        private static void OnAsyncMethodFailed(Task task)
        {
            Console.WriteLine(task.Exception?.ToString() ?? "null error");
        }

        private static readonly ConcurrentQueue<string> eventQueue = new ConcurrentQueue<string>();
        private const string EventHubsConnectionString = "<Your EventHubs connection>"; // TODO: move to config
    }
}
