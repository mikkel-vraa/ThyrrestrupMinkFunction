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
using System.Collections;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace MinkFunctionVisualStudio
{
    public static class Function1
    {
        public static HttpClient client = new HttpClient();
        [FunctionName("Function1")]
        //Creating a connection to the IoThub trigger
        public static async Task Run([IoTHubTrigger("messages/events", Connection = "ConnectionString")] EventData message, ILogger log)
        {
            // Creating a string to represent the data and print a message in the Azure log
            string messageString = Encoding.UTF8.GetString(message.Body.Array);
            var alarms = "alarm";
            var hydTemp = "0";
            var motTemp = "0";
            var unconvTime = "01/01/10 - 10:10:10";
            var convTime = "01/01/10 - 10:10:10";
            var fuelLevel = "0";
            var hydPressure = "0";
            var motorRunTimerHour = "0";
            var RPM = "0";
            string storageConnectionString = $"DefaultEndpointsProtocol=https;AccountName=storageaccountthyrrbb40;AccountKey=QIYoOZF9hFOofKkNXves5ThvoxPmTuaobV0NNgioAB4dGCigabSEMQZTEcdkqsaRcH2Repv4K56gbT1jdVAF/Q==;EndpointSuffix=core.windows.net";
            string containerName = $"blob-container";
            string fileName = $"blobmessage.txt";

            // Setup the connection to the storage account
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            // Connect to the blob storage
            CloudBlobClient serviceClient = storageAccount.CreateCloudBlobClient();
            // Connect to the blob container
            CloudBlobContainer container = serviceClient.GetContainerReference($"{containerName}");
            // Connect to the blob file
            CloudBlockBlob blob = container.GetBlockBlobReference($"{fileName}");
            // overwrite the blob file as text
            string blobcontent = blob.DownloadTextAsync().Result;

            //log.LogInformation($"{blobcontent}");
            log.LogInformation($"C# IoT Hub trigger function processed a message: {messageString}");
            // The content of the message string gets divided between the variables

            //var feedLevel = JObject.Parse(messageString)["feedLevel"].ToString().Replace(',', '.');
            //var motSpeed = JObject.Parse(messageString)["motorSpeed"].ToString().Replace(',', '.');
            //var timeSinceHydService = JObject.Parse(messageString)["timeSinceHydService"].ToString().Replace(',', '.');
            //var timeSinceMotService = JObject.Parse(messageString)["timeSinceMotService"].ToString().Replace(',', '.');
            //var mechanicalMotorTimer = JObject.Parse(messageString)["mechanicalMotorTimer"].ToString().Replace(',', '.');
            //var motRunTimerMinutes = JObject.Parse(messageString)["motRunTimerMinutes"].ToString().Replace(',', '.');

            if (blobcontent.Equals(messageString))
            {
                log.LogInformation($"HAHA NOTHING HAPPENED!!!!");
            }
            else
            {
                if (messageString.Contains("alarms"))
                {
                    log.LogInformation($"alarms is found");
                    alarms = JObject.Parse(messageString)["alarms"].ToString().Replace(',', ' ');
                }
                else
                {
                    log.LogInformation($"alarms is not found");
                    alarms = JObject.Parse(blobcontent)["alarms"].ToString().Replace(',', ' ');
                }

                if (messageString.Contains("hydraulicTemp"))
                {
                    log.LogInformation($"hydraulicTemp is found");
                    hydTemp = JObject.Parse(messageString)["hydraulicTemp"].ToString().Replace(',', '.');
                }
                else
                {
                    log.LogInformation($"hydraulicTemp is not found");
                    hydTemp = JObject.Parse(blobcontent)["hydraulicTemp"].ToString().Replace(',', ' ');
                }

                if (messageString.Contains("motorTemp"))
                {
                    log.LogInformation($"motorTemp is found");
                    motTemp = JObject.Parse(messageString)["motorTemp"].ToString().Replace(',', '.');
                }
                else
                {
                    log.LogInformation($"motorTemp is not found");
                    motTemp = JObject.Parse(blobcontent)["motorTemp"].ToString().Replace(',', ' ');
                }

                if (messageString.Contains("fuelLevel"))
                {
                    log.LogInformation($"fuelLevel is found");
                    fuelLevel = JObject.Parse(messageString)["fuelLevel"].ToString().Replace(',', '.');
                }
                else
                {
                    log.LogInformation($"fuelLevel is not found");
                    fuelLevel = JObject.Parse(blobcontent)["fuelLevel"].ToString().Replace(',', ' ');
                }

                if (messageString.Contains("hydraulicPressure"))
                {
                    log.LogInformation($"hydraulicPressure is found");
                    hydPressure = JObject.Parse(messageString)["hydraulicPressure"].ToString().Replace(',', '.');
                }
                else
                {
                    log.LogInformation($"hydraulicPressure is not found");
                    hydPressure = JObject.Parse(blobcontent)["hydraulicPressure"].ToString().Replace(',', ' ');
                }

                if (messageString.Contains("RPM"))
                {
                    log.LogInformation($"RPM is found");
                    RPM = JObject.Parse(messageString)["RPM"].ToString().Replace(',', '.');
                }
                else
                {
                    log.LogInformation($"RPM is not found");
                    RPM = JObject.Parse(blobcontent)["RPM"].ToString().Replace(',', '.');
                }

                if (messageString.Contains("motorRunTimerHour"))
                {
                    log.LogInformation($"motorRunTimerHour is found");
                    motorRunTimerHour = JObject.Parse(messageString)["motorRunTimerHour"].ToString().Replace(',', '.');
                }
                else
                {
                    log.LogInformation($"motorRunTimerHour is not found");
                    motorRunTimerHour = JObject.Parse(blobcontent)["motorRunTimerHour"].ToString().Replace(',', '.');
                }

                if (messageString.Contains("nowTime"))
                {
                    log.LogInformation($"nowTime is found");
                    unconvTime = JObject.Parse(messageString)["nowTime"].ToString().Replace(',', '.');
                    DateTime timeDate = DateTime.ParseExact(unconvTime, "dd/MM/yy - HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
                    convTime = timeDate.ToString("yy-MM-dd HH:mm:ss").Replace('.', ':');
                    convTime = "20" + convTime;
                }
                else
                {
                    log.LogInformation($"nowTime is not found");
                    unconvTime = JObject.Parse(blobcontent)["nowTime"].ToString().Replace(',', '.');
                    DateTime timeDate = DateTime.ParseExact(unconvTime, "dd/MM/yy - HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
                    convTime = timeDate.ToString("yy-MM-dd HH:mm:ss").Replace('.', ':');
                    convTime = "20" + convTime;
                }

                if (alarms != "8")
                {
                    string blobMessageString = "{" + $"\"motorTemp\": {motTemp},\"hydraulicTemp\": {hydTemp},\"fuelLevel\": {fuelLevel},\"hydraulicPressure\": {hydPressure},\"alarms\": \"{alarms}\",\"RPM\": {RPM},\"motorRunTimerHour\": {motorRunTimerHour},\"nowTime\": \"{unconvTime}\"" + "}";
                    log.LogInformation($"{blobMessageString}");
                    var blobUploadcontent = (blob.UploadTextAsync(blobMessageString));

                    // Connection to the SQL database is established
                    var str = Environment.GetEnvironmentVariable("sqldb_connection");
                    using (SqlConnection conn = new SqlConnection(str))
                    {
                        conn.Open();
                        // Values from the variables are inserted into a SQL statement
                        var text = "INSERT INTO dbo.vehicleDatas (hydraulicTemperature, motorTemperature, nowTime, fuelLevel, hydraulicPressure, motorSpeed, motorRunTimerHour)" +
                       $"VALUES ({hydTemp}, {motTemp}, '{convTime}', {fuelLevel}, {hydPressure}, {RPM}, {motorRunTimerHour})";
                        //var text = "INSERT INTO dbo.vehicleDatas (hydraulicTemperature, motorTemperature, nowTime, fuelLevel, feedLevel, hydraulicPressure, motorSpeed, timeSinceHydService, timeSinceMotService, mechanicalMotorTimer, motRunTimerMinutes)" +
                        //$"VALUES ({hydtemp}, {mottemp}, '{time}', {fuelLevel}, {feedLevel}, {hydPressure}, {motSpeed}, {timeSinceHydService}, {timeSinceMotService}, {mechanicalMotorTimer}, {motRunTimerHour}, motRunTimerMinutes)";

                        using (SqlCommand cmd = new SqlCommand(text, conn))
                        {
                            // Execute the command and log the # rows affected.
                            var rows = await cmd.ExecuteNonQueryAsync();
                            log.LogInformation($"{rows} rows were updated in vehicleDatas");
                        }
                    }
                }
                if (alarms != JObject.Parse(blobcontent)["alarms"].ToString().Replace(',', ' '))
                {
                    var str1 = Environment.GetEnvironmentVariable("sqldb_connection");
                    using (SqlConnection conn1 = new SqlConnection(str1))
                    {
                        conn1.Open();
                        // Values from the variables are inserted into a SQL statement
                        var text1 = "INSERT INTO dbo.alarms (alarmCode, alarmTime)" +
                        $"VALUES ('{alarms}', '{convTime}')";

                        using (SqlCommand cmd1 = new SqlCommand(text1, conn1))
                        {
                            // Execute the command and log the # rows affected.
                            var rows1 = await cmd1.ExecuteNonQueryAsync();
                            log.LogInformation($"{rows1} rows were updated in alarms");
                        }
                    }
                }

                // Using the API Power BI link to stream data onto a graph
                string powerBiUrl = "https://api.powerbi.com/beta/5b19fed6-6f38-497c-835f-830dd2a2f29f/datasets/16852b13-c287-4477-a387-18cd02067576/rows?noSignUpCheck=1&key=540aAYuKODbkDlGOqY1hf5ImgTvh7Q2HuS2Ab9GWy7wa23XMouB2ZRFgp6iE1GqF0LnWIFPjIMiwb6xgqgt%2BMw%3D%3D";
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
}