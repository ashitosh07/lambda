using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Lambda.Core;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Tangonet_Transaction_Producer;

public class Function
{
    private readonly IAmazonKinesis _kinesisClient;

    public Function()
    {
        _kinesisClient = new AmazonKinesisClient();
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
                StreamName = "transaction-data-stream",
                PartitionKey = "partitionKey",
                Data = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(jsonData))
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
