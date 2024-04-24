using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.KinesisEvents;
using Newtonsoft.Json.Linq;
using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Tangonet_Pirani_Product_Consumer
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
                    var fullDocument = jsonObject["events"][0]["event"]["fullDocument"];
                    var operationType = jsonObject["events"][0]["event"]["operationType"];

                    context.Logger.LogLine($" fullDocument: {fullDocument}");

                    var id = fullDocument["_id"].ToString();
                    var userId = fullDocument["userId"].ToString();

                    if (operationType.ToString() == "insert" || operationType.ToString() == "update")
                    {
                        var productInfo = MapProductInfo(fullDocument, id);
                        var json = Newtonsoft.Json.JsonConvert.SerializeObject(productInfo);

                        context.Logger.LogLine($"Mapped transaction data: {json}");

                        var partitionKey = record.Kinesis.PartitionKey;

                        await SaveToDynamoDB(partitionKey, json, context, id, userId);

                        if (operationType.ToString() == "insert")
                        {
                            await PushDataToPirani(json, context);
                        }
                        else if (operationType.ToString() == "update")
                        {
                            await PutDataToPirani(json, context);
                        }

                        context.Logger.LogLine("SAVED");
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

        private ProductInfo MapProductInfo(JToken fullDocument, string id)
        {
            var details = fullDocument["details"];
            var sendInfo = details["sendInfo"];

            return new ProductInfo
            {
                IdentificationType = fullDocument["document_type"]?.ToString() ?? "Default",
                IdentificationNumber = fullDocument["document_number"]?.ToString() ?? "Default",
                ProductNumber = "prod-" + id,
                Product = "Money Transfer",
                Subproduct = sendInfo["send"]?.ToString() ?? "Default",
                RegistrationDate = DateTime.Now,
                City = "New York City",
                BranchOffice = "Center",
                DistributionChannel = "ATM",
              //  CurrencyType = "Dolar",
               // OpeningAmount = "default",
                ProductState = "1",
                ParentType = "COUNTERPARTIES",
            };

        }

        private async Task SaveToDynamoDB(string partitionKey, string data, ILambdaContext context, string id, string userId)
        {
            var table = Table.LoadTable(_dynamoDBClient, _tableName);

            var jsonObject = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, object>>(data);

            if (!jsonObject.ContainsKey("id"))
            {
                jsonObject["id"] = id;
            }

            if (!jsonObject.ContainsKey("user_id"))
            {
                jsonObject["user_id"] = userId;
            }

            var document = new Document();
            document["PartitionKey"] = partitionKey;
            document["id"] = new Primitive(jsonObject["id"].ToString());
            document["user_id"] = new Primitive(jsonObject["user_id"].ToString());

            foreach (var kvp in jsonObject)
            {
                // Skip id and user_id since they are already added to the document
                if (kvp.Key != "id" && kvp.Key != "user_id")
                {
                    document[kvp.Key] = kvp.Value.ToString();
                }
            }

            try
            {
                await table.PutItemAsync(document);
                context.Logger.LogLine("Item saved to DynamoDB successfully.");
            }
            catch (Exception ex)
            {
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
                context.Logger.LogLine($"Error posting data to endpoint: {ex.Message}");
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

                    var response = await httpClient.PutAsync("https://c2mwgtg0k0.execute-api.us-east-1.amazonaws.com/pirani-aml-stage/aml-api/entity/products", content);

                    response.EnsureSuccessStatusCode();
                    var responseContent = await response.Content.ReadAsStringAsync();

                    context.Logger.LogLine($"API Response: {responseContent}");
                }
            }
            catch (Exception ex)
            {
                context.Logger.LogLine($"Error putting data to endpoint: {ex.Message}");
            }
        }
    }

   
}
