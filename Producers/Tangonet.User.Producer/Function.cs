using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Lambda.Core;
using Newtonsoft.Json.Linq;
using System.Text;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Tangonet_User_Producer;

public class Function
{
    private readonly IAmazonKinesis _kinesisClient;
    private readonly string _streamName;
    private readonly string _partitionKey;

    public Function()
    {
        _kinesisClient = new AmazonKinesisClient();
        _streamName = GetConfigValue("StreamName");
        _partitionKey = GetConfigValue("PartitionKey");
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
    public async Task FunctionHandler(Stream documentDbEvent, ILambdaContext context)
    {
        context.Logger.LogLine("STARTING THE FUNCTION");
        try
        {
            using (var reader = new StreamReader(documentDbEvent))
            {
                var eventsArrayString = reader.ReadToEnd();
                context.Logger.LogLine("STARTING THE EVENT ARRAYS");

                await PushDataToKinesis(eventsArrayString, context);
            }
        }
        catch (Exception ex)
        {
            context.Logger.LogLine($"An unexpected exception occurred: {ex}");
        }
    }

    private async Task PushDataToKinesis(string jsonData, ILambdaContext context)
    {
        try
        {
            var putRecordRequest = new PutRecordRequest
            {
                StreamName = _streamName,
                PartitionKey = _partitionKey,
                Data = new MemoryStream(Encoding.UTF8.GetBytes(jsonData))
            };

            var response = await _kinesisClient.PutRecordAsync(putRecordRequest);
            context.Logger.LogLine($"Successfully put record into Kinesis. ShardId: {response}");
            context.Logger.LogLine($"Successfully put record into Kinesis. ShardId: {response.ShardId}");
        }
        catch (Exception ex)
        {
            context.Logger.LogLine($"Failed to put record into Kinesis: {ex.Message}");
        }
    }
}
