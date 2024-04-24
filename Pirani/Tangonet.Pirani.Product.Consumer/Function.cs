using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.KinesisEvents;
using Newtonsoft.Json.Linq;
using System.Text;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Tangonet_Pirani_Product_Consumer
{
    public partial class Function
    {
        private readonly IAmazonDynamoDB _dynamoDBClient;
        private readonly string _tableName;
        private readonly string _apiKey;
        private readonly string _apiValue;
        private readonly string _postEndpoint;
        private readonly string _putEndpoint;

        public Function()
        {
            _dynamoDBClient = new AmazonDynamoDBClient();
            _tableName = GetConfigValue("DynamoDBTableName");
            _apiKey = GetConfigValue("APIKey");
            _apiValue = GetConfigValue("APIValue");
            _postEndpoint = GetConfigValue("PostEndpoint");
            _putEndpoint = GetConfigValue("PutEndpoint");
        }

        private string GetConfigValue(string key)
        {
            try
            {
              
                var jsonConfig = JObject.Parse(File.ReadAllText("aws-lambda-tools-defaults.json"));
                return jsonConfig["Values"][key]?.ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading configuration value for {key}: {ex.Message}");
                return null;
            }
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
                        var productInfo = MapProductInfo(fullDocument);
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

        private ProductInfo MapProductInfo(JToken fullDocument)
        {
            var details = fullDocument["details"];
            var sendAmounts = details["sendInfo"]["sendAmounts"];
            var id = fullDocument["_id"].ToString();
            var userId = fullDocument["userId"].ToString();
            var product = "Remittance";
            var sendInfo = details["sendInfo"];

            return new ProductInfo
            {
                IdentificationType = fullDocument["document_type"]?.ToString() ?? "Driving Licence",
                IdentificationNumber = fullDocument["document_number"]?.ToString() ?? "Default",
                Product = "Remittance",
                Subproduct = sendInfo["send"]?.ToString() ?? "Send",
                ProductNumber = "user-"+ (product == "Remittance" ? 1 : 2),
                //RegistrationDate = DateTime.ParseExact(transactionDate, "yyyyMMddHHmmss", CultureInfo.InvariantCulture),
                RegistrationDate = DateTime.Parse(DateTime.Now.ToString()).ToString("yyyy-MM-ddTHH:mm:ss"),
                City = "New York City",
                BranchOffice = "West",
                DistributionChannel = "ATM",
                CurrencyType = sendAmounts["sendCurrency"].ToString() ?? "Default",
                OpeningAmount = 0,
                ProductState = "1",
                ParentType = "CLIENTS",
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
                    httpClient.DefaultRequestHeaders.Add(_apiKey, _apiValue);

                    var content = new StringContent(data, Encoding.UTF8, "application/json");

                    var response = await httpClient.PostAsync(_postEndpoint, content);

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
                    httpClient.DefaultRequestHeaders.Add(_apiKey, _apiValue);

                    var content = new StringContent(data, Encoding.UTF8, "application/json");

                    var response = await httpClient.PutAsync(_putEndpoint, content);

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
