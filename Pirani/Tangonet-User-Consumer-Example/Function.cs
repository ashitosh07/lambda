using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.KinesisEvents;
using Newtonsoft.Json.Linq;
using System.Text;
using System.Text.Json;
using Document = Amazon.DynamoDBv2.DocumentModel.Document;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Tangonet_Pirani_User_Consumer_Example
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
                    var document = Document.FromJson(data);

                    // Get the fullDocument object
                    var fullDocument = jsonObject["events"][0]["event"]["fullDocument"];
                    var operationType = jsonObject["events"][0]["event"]["operationType"];


                    // Convert the fullDocument to a string
                    string fullDocumentJson = fullDocument.ToString();

                    var onfidoResult = fullDocument["onfidoResult"];
                    var output = onfidoResult["output"];
                    var procD = fullDocument["procDate"];


                    if (operationType.ToString() == "insert")
                    {
                        // Map JSON data to C# model
                        var identification = new ClientInfo
                        {
                            identificationType = output["document_type"]?.ToString() ?? "Default",
                            identificationNumber = output["document_number"]?.ToString() ?? "Default",
                            personType = output["personType"]?.ToString() ?? "Default",
                            firstName = output["first_name"]?.ToString() ?? "Default",
                            firstLastName = output["last_name"]?.ToString() ?? "Default",
                            bomCountry = output["bomCountry"]?.ToString() ?? "Default",
                            bomCity = output["bomCity"]?.ToString() ?? "Default",
                            sex = output["sex"]?.ToString() ?? "Default",
                            economicActivity = output["economicActivity"]?.ToString() ?? "Default",
                            registrationDate = ((DateTime)procD["$date"]),
                            state = output["state"]?.ToString() ?? "Default",
                            businessName = output["businessName"]?.ToString() ?? "Default",
                            updateAt = ((DateTime)onfidoResult["updatedDate"]),
                        };

                        // Serialize the object to JSON
                        var json = JsonSerializer.Serialize(identification);
                        context.Logger.LogLine($"DATA : {json}");

                        ////// Check if the document contains "id" attribute, generate if missing
                        if (!document.Contains("id"))
                        {
                            // Generate unique id
                            var id = Guid.NewGuid().ToString();
                            document["id"] = id;
                        }

                        //// Check if the document contains "user_id" attribute, generate if missing
                        if (!document.Contains("user_id"))
                        {
                            // Generate unique user_id
                            var userId = Guid.NewGuid().ToString();
                            document["user_id"] = userId;
                        }
                        context.Logger.LogLine($"DOCUMENT DATA : {document}");

                        // Get the partition key from the Kinesis record
                        var partitionKey = record.Kinesis.PartitionKey;

                        await SaveToDynamoDB(partitionKey, document);

                        await PushDataToPirani(json, context);

                        context.Logger.LogLine("SAVED");
                    }
                    else
                    {
                        context.Logger.LogLine("Operation is not 'insert'. Skipping record.");
                    }

                }
                catch (Exception ex)
                {
                    context.Logger.LogLine($"Error processing record: {ex.Message}");
                }
            }
        }
        private async Task SaveToDynamoDB(string partitionKey, Document document)
        {
            var table = Table.LoadTable(_dynamoDBClient, _tableName);

            var item = new Document();
            // Use partitionKey as the primary key
            item["PartitionKey"] = partitionKey;
            foreach (var attribute in document.GetAttributeNames())
            {
                item[attribute] = document[attribute];
            }

            await table.PutItemAsync(item);
        }
        private async Task PushDataToPirani(string data, ILambdaContext context) // Add ILambdaContext parameter
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
    }
}