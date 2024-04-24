using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.KinesisEvents;
using Newtonsoft.Json.Linq;
using System.Globalization;
using System.Text;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Tangonet_Pirani_Product_Consumer_Example
{
    public partial class Function
    {
        private readonly IAmazonDynamoDB _dynamoDBClient;
        private readonly string _tableName;

        public Function()
        {
            _dynamoDBClient = new AmazonDynamoDBClient();
            _tableName = "aml-product-data-capture-audits";
        }

        public async Task FunctionHandler(KinesisEvent kinesisEvent, ILambdaContext context)
        {
            if (kinesisEvent == null || kinesisEvent.Records == null)
            {
                context.Logger.LogLine("Kinesis event   or its records are null.");
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
                    var fullDocument = jsonObject["details"];
                    context.Logger.LogLine($" fullDocument: {fullDocument}");
                    context.Logger.LogLine($" jsonObject: {jsonObject}");
                    var transactionDate = jsonObject["transactionDate"]?.ToString() ?? "Default";
                    var transactionType = jsonObject["transactionType"]?.ToString() ?? "Default";
                    var totalAmount = jsonObject["totalAmount"]?.ToString() ?? "Default";
                    context.Logger.LogLine($" totalAmount: {totalAmount}");
                    var machineId = jsonObject["machineId"]?.ToString() ?? "Default";
                    var partnerId = jsonObject["partnerId"]?.ToString() ?? "Default";

                    var sendInfo = fullDocument["sendInfo"];
                    var receiverInfo = fullDocument["receiveInfo"];
                    context.Logger.LogLine($" sendInfo: {sendInfo}");
                    context.Logger.LogLine($" receiverInfo: {receiverInfo}");
                    // Map JSON data to C# model
                    var productInfo = new ProductInfo
                    {
                        Product = "default",
                        Subproduct = "default",
                        ProductState = "1",
                        ProductNumber = "21",
                        RegistrationDate= DateTime.ParseExact(transactionDate, "yyyyMMddHHmmss", CultureInfo.InvariantCulture),
                        City="default",
                        BranchOffice="default",
                        DistributionChannel = "default",
                        IdentificationType ="1",
                        IdentificationNumber = "1",
                        ParentType = "COUNTERPARTIES",
                    };

                    var json = System.Text.Json.JsonSerializer.Serialize(productInfo, new System.Text.Json.JsonSerializerOptions { IgnoreNullValues = true });

                    context.Logger.LogLine($"Mapped transaction data: {json}");

                    var partitionKey = record.Kinesis.PartitionKey;



                    await SaveToDynamoDB(partitionKey, json, context);

                    await PushDataToPirani(json, context);

                    context.Logger.LogLine("SAVED");
                }
                catch (Exception ex)
                {
                    context.Logger.LogLine($"Error processing record: {ex.Message}");
                }
            }
        }
        private async Task SaveToDynamoDB(string partitionKey, string data, ILambdaContext context)
        {
            var table = Table.LoadTable(_dynamoDBClient, _tableName);

            // Deserialize the JSON string into a dictionary
            var jsonObject = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(data);

            // Check if the dictionary contains "id" attribute, generate if missing
            if (!jsonObject.ContainsKey("id"))
            {
                // Generate unique id
                var id = Guid.NewGuid().ToString();
                jsonObject["id"] = id;
            }

            // Check if the dictionary contains "user_id" attribute, generate if missing
            if (!jsonObject.ContainsKey("user_id"))
            {
                // Generate unique user_id
                var userId = Guid.NewGuid().ToString();
                jsonObject["user_id"] = userId;
            }

            // Create a DynamoDB Document from the modified JSON
            var document = new Document();

            // Add the PartitionKey to the document
            document["PartitionKey"] = partitionKey;

            // Add the TransactionData to the document
            document["TransactionData"] = data;

            // Convert the 'id' and 'user_id' to DynamoDBEntry and add to the document
            document["id"] = new Primitive(jsonObject["id"].ToString());
            document["user_id"] = new Primitive(jsonObject["user_id"].ToString());

            try
            {
                // Store the document in DynamoDB
                await table.PutItemAsync(document);

                // Log success
                context.Logger.LogLine("Item saved to DynamoDB successfully.");
            }
            catch (Exception ex)
            {
                // Log error
                context.Logger.LogLine($"Error saving item to DynamoDB: {ex.Message}");
            }
        }




        private async Task PushDataToPirani(string data, ILambdaContext context)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    httpClient.DefaultRequestHeaders.Add("x-api-key", "PN.L6t5wbN6Mtj7.Z2n-5NzS2GkGqG9qwmKllwd-IC01u7kQRb5Flb_xAP9Nlwbl");

                    var content = new StringContent(data, Encoding.UTF8, "application/json");

                    var response = await httpClient.PostAsync("https://c2mwgtg0k0.execute-api.us-east-1.amazonaws.com/pirani-aml-stage/aml-api/entity/products", content);

                    response.EnsureSuccessStatusCode();
                    var responseContent = await response.Content.ReadAsStringAsync();

                    context.Logger.LogLine($"API Response: {responseContent}");
                }
            }
            catch (Exception ex)
            {
                context.Logger.LogLine("Error posting data to endpoint: " + ex.Message);
            }
        }
    }


}
