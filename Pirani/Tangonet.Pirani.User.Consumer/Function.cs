using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.KinesisEvents;
using Newtonsoft.Json.Linq;
using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Tangonet_Pirani_User_Consumer
{
    public partial class Function
    {
        private readonly IAmazonDynamoDB _dynamoDBClient;
        private readonly string _tableName;

        public Function()
        {
            _dynamoDBClient = new AmazonDynamoDBClient();
            _tableName = "aml-user-data-capture-audits";
        }

        public async Task FunctionHandler(KinesisEvent kinesisEvent, ILambdaContext context)
        {
            if (kinesisEvent == null || kinesisEvent.Records == null)
            {
                context.Logger.LogLine("Kinesis event or its records are null.");
                return;
            }

            foreach (var record in kinesisEvent.Records)
            {
                if (record == null || record.Kinesis == null || record.Kinesis.Data == null)
                {
                    context.Logger.LogLine("Record or its properties are null.");
                    continue;
                }

                var data = Encoding.UTF8.GetString(record.Kinesis.Data.ToArray());
                context.Logger.LogLine($"Decoded string: {data}");

                try
                {
                    var jsonObject = JObject.Parse(data);
                    var operationType = jsonObject["events"][0]["event"]["operationType"];

                    // Check the operation type
                    if (operationType.ToString() == "insert" || operationType.ToString() == "update")
                    {
                        var commonData = PrepareCommonData(data, jsonObject);

                        if (operationType.ToString() == "insert")
                        {
                            await PushDataToPirani(commonData, context);
                        }
                        else if (operationType.ToString() == "update")
                        {
                            await PutDataToPirani(commonData, context);
                        }

                        await SaveToDynamoDB(record.Kinesis.PartitionKey, JObject.Parse(data));
                    }
                    else
                    {
                        context.Logger.LogLine("Operation is not 'insert' or 'update'. Skipping record.");
                    }
                }
                catch (Exception ex)
                {
                    context.Logger.LogLine($"Error processing record: {ex.Message}");
                }
            }
        }

        private string PrepareCommonData(string data, JObject jsonObject)
        {
            var fullDocument = jsonObject["events"][0]["event"]["fullDocument"];
            var procD = fullDocument["procDate"];

            var onfidoResult = fullDocument["onfidoResult"];
            var output = onfidoResult["output"];

            // Map JSON data to C# model
            var identification = new ClientInfo
            {
                identificationType = output["document_type"]?.ToString() ?? "default",
                identificationNumber = output["document_number"]?.ToString() ?? "default",
                personType = output["personType"]?.ToString() ?? "1",
                firstName = output["first_name"]?.ToString() ?? "default",
                firstLastName = output["last_name"]?.ToString() ?? "default",
                bomCountry = output["bomCountry"]?.ToString() ?? "default",
                bomCity = output["bomCity"]?.ToString() ?? "default",
                sex = output["sex"]?.ToString() ?? "2",
                economicActivity = output["economicActivity"]?.ToString() ?? "default",
                registrationDate = ((DateTime)procD["$date"]).ToString("yyyy-MM-ddTHH:mm:ss"),
                state = output["state"]?.ToString() ?? "1",
                businessName = output["businessName"]?.ToString() ?? "default",
                updateAt = ((DateTime)onfidoResult["updatedDate"]).ToString("yyyy-MM-ddTHH:mm:ss"),
            };

            // Serialize the object to JSON
            return JsonSerializer.Serialize(identification);
        }

        private async Task PushDataToPirani(string data, ILambdaContext context)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    httpClient.DefaultRequestHeaders.Add("x-api-key", "PN.L6t5wbN6Mtj7.Z2n-5NzS2GkGqG9qwmKllwd-IC01u7kQRb5Flb_xAP9Nlwbl");

                    var content = new StringContent(data, Encoding.UTF8, "application/json");

                    var response = await httpClient.PostAsync("https://c2mwgtg0k0.execute-api.us-east-1.amazonaws.com/pirani-aml-stage/aml-api/entity/clients", content);

                    response.EnsureSuccessStatusCode();
                    var responseContent = await response.Content.ReadAsStringAsync();

                    // Log message using context object
                    context.Logger.LogLine($"API Response: {responseContent}");
                    Console.WriteLine("API Response: " + responseContent);
                }
            }
            catch (Exception ex)
            {
                // Log error using context object
                context.Logger.LogLine("Error posting data to endpoint: " + ex.Message);
                Console.WriteLine("Error posting data to endpoint: " + ex.Message);
            }
        }

        private async Task SaveToDynamoDB(string partitionKey, JObject document)
        {
            try
            {
                var table = Table.LoadTable(_dynamoDBClient, _tableName);

                var item = new Document();
                // Use partitionKey as the primary key
                item["PartitionKey"] = partitionKey;
                foreach (var attribute in document)
                {
                    item[attribute.Key] = attribute.Value.ToString();
                }

                await table.PutItemAsync(item);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error saving to DynamoDB: {ex.Message}");
                throw;
            }
        }

        private async Task PutDataToPirani(string data, ILambdaContext context)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    httpClient.DefaultRequestHeaders.Add("x-api-key", "PN.L6t5wbN6Mtj7.Z2n-5NzS2GkGqG9qwmKllwd-IC01u7kQRb5Flb_xAP9Nlwbl");

                    var content = new StringContent(data, Encoding.UTF8, "application/json");

                    var response = await httpClient.PutAsync("https://c2mwgtg0k0.execute-api.us-east-1.amazonaws.com/pirani-aml-stage/aml-api/entity/clients", content);

                    response.EnsureSuccessStatusCode();
                    var responseContent = await response.Content.ReadAsStringAsync();

                    // Log message using context object
                    context.Logger.LogLine($"API Response: {responseContent}");
                    Console.WriteLine("API Response: " + responseContent);
                }
            }
            catch (Exception ex)
            {
                // Log error using context object
                context.Logger.LogLine("Error posting data to endpoint: " + ex.Message);
                Console.WriteLine("Error posting data to endpoint: " + ex.Message);
            }
        }
    }

   
}
