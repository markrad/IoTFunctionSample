using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace IoTFunctionSample
{
    public static class Function1
    {
        private static DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private static EventHubClient eventHubClient;

        // Constructor: Set up connection to target Event Hub
        static Function1()
        {
            try
            {
                string eventHubConnectionString = Environment.GetEnvironmentVariable("TargetEventHubSpace", EnvironmentVariableTarget.Process);
                string eventHubName = Environment.GetEnvironmentVariable("TargetEventHubName", EnvironmentVariableTarget.Process);
                EventHubsConnectionStringBuilder eventHubsConnectionStringBuilder = new EventHubsConnectionStringBuilder(eventHubConnectionString)
                {
                    EntityPath = eventHubName
                };

                eventHubClient = EventHubClient.CreateFromConnectionString(eventHubsConnectionStringBuilder.ToString());
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        [FunctionName("Function1")]
        // Called by Azure each time one or more messages is available at the IoT hub
        // Consumer group is optional but an Azure function will take exclusive control of the consumer group
        public static void Run([EventHubTrigger("messages/events", Connection = "ConnectionString", ConsumerGroup = "aa")] EventData[] events, ILogger log)
        {
            var x = Environment.GetEnvironmentVariable("TargetEventHubName", EnvironmentVariableTarget.Process);
            string data = "";

            foreach (EventData message in events)
            {
                try
                {
                    // Manipulate, log and send data to Event Hub
                    data = Encoding.UTF8.GetString(message.Body.Array);
                    JArray o = JArray.Parse(data);
                    DateTime messageTime = epoch.AddSeconds(o[0]["magictime"].Value<double>()).ToLocalTime();
                    log.LogInformation("Time = " + messageTime.ToString() + " Enqueued Time = " + message.SystemProperties.EnqueuedTimeUtc.ToLocalTime().ToString());
                    SendMessage(messageTime.ToString()).Wait();
                }
                catch (Exception e)
                {
                    log.LogWarning($"Exception {e.Message}");
                    log.LogWarning($"Unable to parse JSON for {data}");
                }
            }
        }

        // Send data to the target Event Hub
        private static async Task SendMessage(string message)
        {
            string jsonStr = $"{{ \"message\" : \"{message}\" }}";
            await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(jsonStr)));
        }
    }
}
