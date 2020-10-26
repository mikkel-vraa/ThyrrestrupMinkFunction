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
        [FunctionName("Function1")]
        //Creating a connection to the IoThub trigger
        public static async Task Run([IoTHubTrigger("messages/events", Connection = "ConnectionString")] EventData message, ILogger log)
        {
            // Creating a string to represent the data and print a message in the Azure log
            string messageString = Encoding.UTF8.GetString(message.Body.Array);
            var alarms = "0";
            var hydTemp = "0";
            var motTemp = "0";
            var unconvTime = "01/01/10 - 10:10:10";
            var convTime = "01/01/10 - 10:10:10";
            var fuelLevel = "0";
            var hydPressure = "0";
            var motorRunTimerHour = "0";
            var RPM = "0";
            var mechanicalMotorTimer = "0";
            var timeSinceHydService = "0";
            var timeSinceMotService = "0";
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
            // read the blob file
            string blobcontent = blob.DownloadTextAsync().Result;

            //log.LogInformation($"{blobcontent}");
            log.LogInformation($"C# IoT Hub trigger function processed a message: {messageString}");
            // The content of the message string gets divided between the variables
            log.LogInformation("Alarms: {alarms} HydraulicTemp: {hydTemp} MotorTemp: {motorTemp} FuelLevel: {fuelLevel} " +
                "HydraulicPressure: {hydraulicPressure} RPM: {RPM} motorRunTimerHour: {motorRunTimerHour} " +
                "mechanicalMotorTimer: {mechanicalMotorTimer} TimeSinceHydServ: {timeSinceHydService} " +
                "TimeSinceMotServ: {timeSinceMotService} unconvTime: {unconvTime}",
            messageString.Contains("alarms") ?
            alarms = JObject.Parse(messageString)["alarms"].ToString().Replace(',', ' ') :
            alarms = JObject.Parse(blobcontent)["alarms"].ToString().Replace(',', ' '),
            messageString.Contains("hydraulicTemp") ?
            hydTemp = JObject.Parse(messageString)["hydraulicTemp"].ToString().Replace(',', '.') :
            hydTemp = JObject.Parse(blobcontent)["hydraulicTemp"].ToString().Replace(',', ' '),
            messageString.Contains("motorTemp") ?
            motTemp = JObject.Parse(messageString)["motorTemp"].ToString().Replace(',', '.') :
            motTemp = JObject.Parse(blobcontent)["motorTemp"].ToString().Replace(',', ' '),
            messageString.Contains("fuelLevel") ?
            fuelLevel = JObject.Parse(messageString)["fuelLevel"].ToString().Replace(',', '.') :
            fuelLevel = JObject.Parse(blobcontent)["fuelLevel"].ToString().Replace(',', ' '),
            messageString.Contains("hydraulicPressure") ?
            hydPressure = JObject.Parse(messageString)["hydraulicPressure"].ToString().Replace(',', '.') :
            hydPressure = JObject.Parse(blobcontent)["hydraulicPressure"].ToString().Replace(',', ' '),
            messageString.Contains("RPM") ?
            RPM = JObject.Parse(messageString)["RPM"].ToString().Replace(',', '.') :
            RPM = JObject.Parse(blobcontent)["RPM"].ToString().Replace(',', '.'),
            messageString.Contains("motorRunTimerHour") ?
            motorRunTimerHour = JObject.Parse(messageString)["motorRunTimerHour"].ToString().Replace(',', '.') :
            motorRunTimerHour = JObject.Parse(blobcontent)["motorRunTimerHour"].ToString().Replace(',', '.'),
            messageString.Contains("mechanicalMotorTimer") ?
            mechanicalMotorTimer = JObject.Parse(messageString)["mechanicalMotorTimer"].ToString().Replace(',', '.') :
            mechanicalMotorTimer = JObject.Parse(blobcontent)["mechanicalMotorTimer"].ToString().Replace(',', '.'),
            messageString.Contains("TimeSinceHydServ") ?
            timeSinceHydService = JObject.Parse(messageString)["TimeSinceHydServ"].ToString().Replace(',', '.') :
            timeSinceHydService = JObject.Parse(blobcontent)["TimeSinceHydServ"].ToString().Replace(',', '.'),
            messageString.Contains("TimeSinceMotServ") ?
            timeSinceMotService = JObject.Parse(messageString)["TimeSinceMotServ"].ToString().Replace(',', '.') :
            timeSinceMotService = JObject.Parse(blobcontent)["TimeSinceMotServ"].ToString().Replace(',', '.'),
            messageString.Contains("nowTime") ?
            unconvTime = JObject.Parse(messageString)["nowTime"].ToString().Replace(',', '.') :
            unconvTime = JObject.Parse(blobcontent)["nowTime"].ToString().Replace(',', '.'));
            DateTime timeDate = DateTime.ParseExact(unconvTime, "dd/MM/yy - HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
            convTime = timeDate.ToString("yy-MM-dd HH:mm:ss").Replace('.', ':');
            convTime = "20" + convTime;

            await DataSQL(motTemp, hydTemp, fuelLevel, hydPressure, alarms, RPM, motorRunTimerHour, mechanicalMotorTimer, timeSinceHydService, timeSinceMotService, unconvTime, convTime, log, blob);
            await alarmSQL(blobcontent, alarms, convTime, log);
            await PowerBIStream(messageString, log);
        }
        public static async Task DataSQL(string motTemp, string hydTemp, string fuelLevel, string hydPressure, string alarms, string RPM, string motorRunTimerHour, string mechanicalMotorTimer, string timeSinceHydService, string timeSinceMotService, string unconvTime, string convTime, ILogger log, CloudBlockBlob blob)
        {
            if (alarms != "8")
            {
                string blobMessageString = "{" + $"\"motorTemp\": {motTemp},\"hydraulicTemp\": {hydTemp},\"fuelLevel\": {fuelLevel},\"hydraulicPressure\": {hydPressure},\"alarms\": \"{alarms}\",\"RPM\": {RPM},\"motorRunTimerHour\": {motorRunTimerHour},\"mechanicalMotorTimer\": {mechanicalMotorTimer},\"TimeSinceHydServ\": {timeSinceHydService},\"TimeSinceMotServ\": {timeSinceMotService},\"nowTime\": \"{unconvTime}\"" + "}";
                log.LogInformation($"{blobMessageString}");
                var blobUploadcontent = (blob.UploadTextAsync(blobMessageString));

                // Connection to the SQL database is established
                var str = Environment.GetEnvironmentVariable("sqldb_connection");
                using SqlConnection conn = new SqlConnection(str);
                conn.Open();
                // Values from the variables are inserted into a SQL statement
                var text = "INSERT INTO dbo.vehicleDatas (hydraulicTemperature, motorTemperature, nowTime, fuelLevel, hydraulicPressure, motorSpeed, motorRunTimerHour, mechanicalMotorTimer, timeSinceHydService, timeSinceMotService )" +
               $"VALUES ({hydTemp}, {motTemp}, '{convTime}', {fuelLevel}, {hydPressure}, {RPM}, {motorRunTimerHour}, {mechanicalMotorTimer}, {timeSinceHydService}, {timeSinceMotService})";
                //var text = "INSERT INTO dbo.vehicleDatas (hydraulicTemperature, motorTemperature, nowTime, fuelLevel, feedLevel, hydraulicPressure, motorSpeed, timeSinceHydService, timeSinceMotService, mechanicalMotorTimer, motRunTimerMinutes)" +
                //$"VALUES ({hydtemp}, {mottemp}, '{time}', {fuelLevel}, {feedLevel}, {hydPressure}, {motSpeed}, {timeSinceHydService}, {timeSinceMotService}, {mechanicalMotorTimer}, {motRunTimerHour}, motRunTimerMinutes)";

                using SqlCommand cmd = new SqlCommand(text, conn);
                // Execute the command and log the # rows affected.
                var rows = await cmd.ExecuteNonQueryAsync();
                log.LogInformation($"{rows} rows were updated in vehicleDatas");
            }
        }
        public static async Task alarmSQL(string blobcontent, string alarms, string convTime, ILogger log)
        {
            if (alarms != JObject.Parse(blobcontent)["alarms"].ToString().Replace(',', ' '))
            {
                var str1 = Environment.GetEnvironmentVariable("sqldb_connection");
                using SqlConnection conn1 = new SqlConnection(str1);
                conn1.Open();
                // Values from the variables are inserted into a SQL statement
                var text1 = "INSERT INTO dbo.alarms (alarmCode, alarmTime)" +
                $"VALUES ('{alarms}', '{convTime}')";

                using SqlCommand cmd1 = new SqlCommand(text1, conn1);
                // Execute the command and log the # rows affected.
                var rows1 = await cmd1.ExecuteNonQueryAsync();
                log.LogInformation($"{rows1} rows were updated in alarms");
            }
        }
        public static async Task PowerBIStream(string messageString, ILogger log)
        {
            // Using the API Power BI link to stream data onto a graph
            string powerBiUrl = "https://api.powerbi.com/beta/5b19fed6-6f38-497c-835f-830dd2a2f29f/datasets/16852b13-c287-4477-a387-18cd02067576/rows?noSignUpCheck=1&key=540aAYuKODbkDlGOqY1hf5ImgTvh7Q2HuS2Ab9GWy7wa23XMouB2ZRFgp6iE1GqF0LnWIFPjIMiwb6xgqgt%2BMw%3D%3D";
            HttpContent content = new StringContent(messageString);
            HttpClient client = new HttpClient();
            try
            {
                HttpResponseMessage response = await client.PostAsync(powerBiUrl, content);
                response.EnsureSuccessStatusCode();
                log.LogInformation($"job's done");
            }
            catch
            {
                Console.WriteLine("HTTP Response failed.");
            }
        }
    }
}