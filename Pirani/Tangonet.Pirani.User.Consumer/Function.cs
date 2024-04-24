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
            var operationType = jsonObject["events"][0]["event"]["operationType"];
            var id = fullDocument["_id"];
            var documentInfo = fullDocument["documentInfo"];
            var address = fullDocument["address"];
            var regDate = fullDocument["regDate"];
            var updDate = fullDocument["updDate"];

            // Map JSON data to C# model
            var identification = new ClientInfo
            {
                identificationType = documentInfo["documentType"]?.ToString() ?? "Driving Licence",
                identificationNumber = documentInfo["documentNo"]?.ToString() ?? "Default",
                personType = fullDocument["personType"]?.ToString() == "Natural" ? "1" : "2" ?? "Default",
                firstName = fullDocument["firstName"]?.ToString() ?? "Default",
                firstLastName = fullDocument["lastName"]?.ToString() ?? "Default",
                businessName = fullDocument["firstName"]?.ToString() + fullDocument["lastName"]?.ToString() ?? "Default",
                constitutionDate = DateTime.Parse(DateTime.Now.ToString()).ToString("yyyy-MM-ddTHH:mm:ss"),
                economicActivity = fullDocument["economicActivity"]?.ToString() ?? "Independant",
                registrationDate = DateTime.Parse(regDate["$date"].ToString()).ToString("yyyy-MM-ddTHH:mm:ss"),
                updateAt = DateTime.Parse(regDate["$date"].ToString()).ToString("yyyy-MM-ddTHH:mm:ss"),
                state = address["state"]?.ToString() == "Active" ? "Active" : "Inactive",
                city = address["city"]?.ToString() ?? "Default",
                country = address["country"]?.ToString() ?? "Default",
                fullAddress = address["fullAddress"]?.ToString() ?? "Default",
                postalCode = fullDocument["postalCode"]?.ToString() ?? "Default",
                phone = fullDocument["homePhone"]?.ToString() ?? "Default",
                email = fullDocument["email"]?.ToString() ?? "Default",
            };

            // Serialize the object to JSON
            return JsonSerializer.Serialize(identification);
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
