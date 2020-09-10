using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Net.Http;
using Microsoft.Extensions.Logging;
using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace MinkFunctionVisualStudio
{
    // The Azure function doesn't work locally and must be used through the Azure Portal to succesfully execute the function
    public static class Function1
    {
        public static HttpClient client = new HttpClient();
        [FunctionName("Function1")]
        //Creating a connection to the IoThub trigger
        public static async Task Run([IoTHubTrigger("messages/events", Connection = "ConnectionString")]EventData message, ILogger log)
        {
            // Creating a string to represent the data and print a message in the Azure log
            string messageString = Encoding.UTF8.GetString(message.Body.Array);
            log.LogInformation($"C# IoT Hub trigger function processed a message: {messageString}");
            Console.WriteLine(messageString);
            // The content of the message string gets divided between the variables 
            var hydtemp = JObject.Parse(messageString)["hydraulicTemp"].ToString().Replace(',', '.');
            var mottemp = JObject.Parse(messageString)["motorTemp"].ToString().Replace(',', '.');
            var time = JObject.Parse(messageString)["nowTime"].ToString().Replace(',', '.');
            DateTime timeDate = DateTime.ParseExact(time, "dd/MM/yy - HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
            time = timeDate.ToString("yy-MM-dd HH:mm:ss").Replace('.', ':');
            time = "20"+time;
            //var fuelLevel = JObject.Parse(messageString)["fuelLevel"].ToString().Replace(',', '.');
            //var feedLevel = JObject.Parse(messageString)["feedLevel"].ToString().Replace(',', '.');
            //var hydPressure = JObject.Parse(messageString)["hydraulicPressure"].ToString().Replace(',', '.');
            //var motSpeed = JObject.Parse(messageString)["motorSpeed"].ToString().Replace(',', '.');
            //var timeSinceHydService = JObject.Parse(messageString)["timeSinceHydService"].ToString().Replace(',', '.');
            //var timeSinceMotService = JObject.Parse(messageString)["timeSinceMotService"].ToString().Replace(',', '.');
            //var mechanicalMotorTimer = JObject.Parse(messageString)["mechanicalMotorTimer"].ToString().Replace(',', '.');
            //var motRunTimerHour = JObject.Parse(messageString)["motRunTimerHour"].ToString().Replace(',', '.');
            //var alarmError = JObject.Parse(messageString)["error"].ToString().Replace(',', '.');
            // Connection to the SQL database is established
            var str = Environment.GetEnvironmentVariable("sqldb_connection");
            using (SqlConnection conn = new SqlConnection(str))
            {
                conn.Open();
                Console.WriteLine("Connection is bueno");
                // Values from the variables are inserted into a SQL statement
                var text = "INSERT INTO dbo.vehicleDatas (hydraulicTemperature, motorTemperature, nowTime)" +
                 $"VALUES ({hydtemp}, {mottemp}, '{time}')";
                //var text = "INSERT INTO dbo.vehicleDatas (hydraulicTemperature, motorTemperature, nowTime, fuelLevel, feedLevel, hydraulicPressure, motorSpeed, timeSinceHydService, timeSinceMotService, mechanicalMotorTimer)" +
                //$"VALUES ({hydtemp}, {mottemp}, '{time}', {fuelLevel}, {feedLevel}, {hydPressure}, {motSpeed}, {timeSinceHydService}, {timeSinceMotService}, {mechanicalMotorTimer}, {motRunTimerHour},)";
                //string binaryAlarmCompare = "1";
                //if (alarmError.Equals(binaryAlarmCompare))
                //{
                //    string text1 = "INSERT INTO dbo.Alarms (error, timestamp)" +
                //$"VALUES ({alarmError}, '{time}')";
                //    using (SqlCommand cmd1 = new SqlCommand(text1, conn));
                //}
                Console.WriteLine(text);
                using (SqlCommand cmd = new SqlCommand(text, conn))
                {
                    // Execute the command and log the # rows affected.
                    var rows = await cmd.ExecuteNonQueryAsync();
                    log.LogInformation($"{rows} rows were updated");
                }
            }

            // Using the API Power BI link to stream data onto a graph
            string powerBiUrl = "https://api.powerbi.com/beta/5b19fed6-6f38-497c-835f-830dd2a2f29f/datasets/a89d129d-dd26-4521-b2df-85448314da2c/rows?noSignUpCheck=1&key=%2FKellOQjOpp%2By2xR0HJUQioITBsd7Zyojehq8ziVMdHueezBKL0yUbImxWe2XsbMx%2FKLsYSxuk%2BvGYmVsIqi1w%3D%3D";
            HttpContent content = new StringContent(messageString);
            
            try
            {
                HttpResponseMessage response = await client.PostAsync(powerBiUrl, content);
                response.EnsureSuccessStatusCode();
            }
            catch
            {
                Console.WriteLine("HTTP Response failed.");
            }
        }
    }
}