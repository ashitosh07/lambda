using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.KinesisEvents;
using Newtonsoft.Json.Linq;
using System;
using System.Globalization;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Tangonet_Pirani_Transaction_Consumer
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
                // Load aws-lambda-tools-defaults.json file
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
                context.Logger.LogLine($"Decoded data: {data}");

                try
                {
                    var jsonObject = JObject.Parse(data);
                    var operationType = jsonObject["events"][0]["event"]["operationType"].ToString();

                    if (operationType == "insert" || operationType == "update")
                    {
                        var transactionInfo = ExtractTransactionData(jsonObject);
                        var commonData = ProcessInsertOperation(transactionInfo, jsonObject, context);

                        if (operationType == "insert")
                            await PushDataToPirani(commonData, context);
                        else if (operationType == "update")
                            await PutDataToPirani(commonData, context);

                        await ProcessAndSaveToDynamoDB(record.Kinesis.PartitionKey, commonData, transactionInfo.Id, transactionInfo.UserId, context);
                    }
                }
                catch (Exception ex)
                {
                    context.Logger.LogLine($"Error processing record: {ex.Message}");
                }
            }
        }

        private TransactionInfo ExtractTransactionData(JObject jsonObject)
        {
            var fullDocument = jsonObject["events"][0]["event"]["fullDocument"];
            var id = fullDocument["_id"].ToString();
            var userId = fullDocument["userId"].ToString();
            var transactionDate = DateTime.ParseExact(fullDocument["transactionDate"].ToString(), "yyyyMMddHHmmss", CultureInfo.InvariantCulture);

            return new TransactionInfo
            {
                Id = id,
                UserId = userId,
                TransactionDate = transactionDate
            };
        }

        private string ProcessInsertOperation(TransactionInfo transactionInfo, JObject jsonObject, ILambdaContext context)
        {
            var fullDocument = jsonObject["events"][0]["event"]["fullDocument"];
            var sendInfo = fullDocument["details"]["sendInfo"];
            var sendAmounts = sendInfo["sendAmounts"];
            var receiveInfo = fullDocument["details"]["receiveInfo"];
            var receiveAmounts = receiveInfo["receiveAmounts"];

            var commonData = new
            {
                TransactionCode = Guid.NewGuid().ToString(),
                ProductNumber = "21",
                TransactionDate = transactionInfo.TransactionDate,
                DistributionOrReceptionChannel = "ATM",
                BranchOffice = "Center",
                City = sendInfo["senderCity"]?.ToString() ?? "Default",
                Country = sendInfo["senderCountry"]?.ToString() ?? "Default",
                OperationValue = sendAmounts["totalAmountToCollect"]?.ToString() ?? "Default",
                Nature = "1",
                CurrencyType = "Dollar",
                TransactionType = fullDocument["transactionType"]?.ToString() ?? "Default",
                TransactionFee = sendAmounts["totalSendFees"]?.ToString() ?? "Default",
                TerminalId = fullDocument["machineId"]?.ToString() ?? "Default",
                PartnerId = fullDocument["partnerId"]?.ToString() ?? "Default",
                TransactionStatus = fullDocument["transactionStatus"]?.ToString() ?? "SUCCESS",
                SenderCurrency = sendAmounts["sendCurrency"]?.ToString() ?? "Default",
                SenderFees = sendAmounts["totalSendFees"]?.ToString() ?? "Default",
                DeliveryOption = receiveInfo["deliveryOption"]?.ToString() ?? "Default",
                ReceiverFirstName = receiveInfo["receiverFirstName"]?.ToString() ?? "Default",
                ReceiverLastName = receiveInfo["receiverLastName"]?.ToString() ?? "Default",
                ReceiveAmount = receiveAmounts["receiveAmount"]?.ToString() ?? "Default",
                ReceiveCurrency = receiveAmounts["receiveCurrency"]?.ToString() ?? "Default",
                DeliveryCountry = receiveInfo["destinationCountry"]?.ToString() ?? "Default",
            };

            return JsonSerializer.Serialize(commonData);
        }

        private async Task ProcessAndSaveToDynamoDB(string partitionKey, string commonData, string id, string userId, ILambdaContext context)
        {
            try
            {
                // Deserialize the commonData string back to TransactionInfo
                var transactionInfo = JsonSerializer.Deserialize<TransactionInfo>(commonData);

                await SaveToDynamoDB(partitionKey, transactionInfo, id, userId, context);
            }
            catch (Exception ex)
            {
                context.Logger.LogLine($"Error processing and saving item to DynamoDB: {ex.Message}");
            }
        }

        private async Task SaveToDynamoDB(string partitionKey, TransactionInfo transactionInfo, string id, string userId, ILambdaContext context)
        {
            // Validation
            if (string.IsNullOrEmpty(partitionKey) || transactionInfo == null || context == null)
            {
                context.Logger.LogLine("Invalid input parameters for saving to DynamoDB.");
                return;
            }

            try
            {
                var table = Table.LoadTable(_dynamoDBClient, _tableName);
                var document = new Document();
                document["PartitionKey"] = partitionKey;
                document["id"] = id;
                document["user_id"] = userId;

              
                foreach (var property in typeof(TransactionInfo).GetProperties())
                {
                    if (property.Name != "Id" && property.Name != "UserId")
                    {
                        var value = property.GetValue(transactionInfo);
                        document[property.Name] = value?.ToString() ?? "Default";
                    }
                }
                await table.PutItemAsync(document);
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
                    context.Logger.LogLine($"Data Response: {data}");
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
                context.Logger.LogLine($"Error posting data to endpoint: {ex.Message}");
            }
        }
    }
}
