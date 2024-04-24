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

        public Function()
        {
            _dynamoDBClient = new AmazonDynamoDBClient();
            _tableName = "aml-transaction-data-capture-audits";
        }

        private async Task FunctionHandler(KinesisEvent kinesisEvent, ILambdaContext context)
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
                    var operationType = jsonObject["events"][0]["event"]["operationType"];

                    if (operationType.ToString() == "insert" || operationType.ToString() == "update")
                    {
                        var (id, userId, senderCity, senderCountry, transactionDate) = ExtractTransactionData(jsonObject);

                        var commonData = ProcessInsertOperation(id, userId, senderCity, senderCountry, transactionDate, context);

                        if (operationType.ToString() == "insert")
                        {
                            await PushDataToPirani(commonData, context);
                        }
                        else if (operationType.ToString() == "update")
                        {
                            await PutDataToPirani(commonData, context);
                        }

                        // Pass the TransactionInfo object to ProcessAndSaveToDynamoDB
                        await ProcessAndSaveToDynamoDB(record.Kinesis.PartitionKey, commonData, id, userId, context);
                    }
                }
                catch (Exception ex)
                {
                    context.Logger.LogLine($"Error processing record: {ex.Message}");
                }
            }
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

        private (string, string, string, string, DateTime) ExtractTransactionData(JObject jsonObject)
        {
            var fullDocument = jsonObject["events"][0]["event"]["fullDocument"];
            var id = fullDocument["_id"].ToString();
            var userId = fullDocument["userId"].ToString();
            var senderInfo = fullDocument["details"]["sendInfo"];
            var senderCity = senderInfo["senderCity"].ToString();
            var senderCountry = senderInfo["senderCountry"].ToString();
            var transactionDate = DateTime.ParseExact(fullDocument["transactionDate"].ToString(), "yyyyMMddHHmmss", CultureInfo.InvariantCulture);
            return (id, userId, senderCity, senderCountry, transactionDate);
        }

        private string ProcessInsertOperation(string id, string userId, string senderCity, string senderCountry, DateTime transactionDate, ILambdaContext context)
        {
            var transactionInfo = new TransactionInfo
            {
                TransactionCode = Guid.NewGuid().ToString(),
                ProductNumber = "Prod-" + id,
                TransactionDate = transactionDate,
                DistributionOrReceptionChannel = "default",
                BranchOffice = "default",
                City = senderCity,
                Country = senderCountry,
                Nature = "1",
                OperationValue = 0,
                CashValue = 0,
                CheckValue = 0,
                ElectronicChannelValue = 1,
                CurrencyType = "default",
            };

            var jsonSerializerOptions = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase // Ensures camelCase property names in JSON
            };

            return JsonSerializer.Serialize(transactionInfo, jsonSerializerOptions);
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

                // Log and store specific fields
                context.Logger.LogLine($"TransactionCode: {transactionInfo.TransactionCode}");
                document["TransactionCode"] = transactionInfo.TransactionCode;

                // Store additional fields
                document["ProductNumber"] = transactionInfo.ProductNumber;
                document["TransactionDate"] = transactionInfo.TransactionDate.ToString("yyyy-MM-ddTHH:mm:ss");
                document["DistributionOrReceptionChannel"] = transactionInfo.DistributionOrReceptionChannel;
                document["BranchOffice"] = transactionInfo.BranchOffice;
                document["City"] = transactionInfo.City;
                document["Country"] = transactionInfo.Country;
                document["Nature"] = transactionInfo.Nature;
                document["OperationValue"] = transactionInfo.OperationValue;
                document["CashValue"] = transactionInfo.CashValue;
                document["CheckValue"] = transactionInfo.CheckValue;
                document["ElectronicChannelValue"] = transactionInfo.ElectronicChannelValue;
                document["CurrencyType"] = transactionInfo.CurrencyType;


                // Store Id and UserId
                document["id"] = id;
                document["user_id"] = userId;

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
                    httpClient.DefaultRequestHeaders.Add("x-api-key", "PN.L6t5wbN6Mtj7.Z2n-5NzS2GkGqG9qwmKllwd-IC01u7kQRb5Flb_xAP9Nlwbl");
                    context.Logger.LogLine($"Data Response: {data}");
                    var content = new StringContent(data, Encoding.UTF8, "application/json");
                    var response = await httpClient.PostAsync("https://c2mwgtg0k0.execute-api.us-east-1.amazonaws.com/pirani-aml-stage/aml-api/entity/transactions", content);
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
                    var response = await httpClient.PutAsync("https://c2mwgtg0k0.execute-api.us-east-1.amazonaws.com/pirani-aml-stage/aml-api/entity/transactions", content);
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
