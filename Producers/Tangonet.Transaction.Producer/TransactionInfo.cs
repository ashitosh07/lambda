using System.Text.Json.Serialization;

namespace dev_tangonet_transaction_producer_lambda;

public class TransactionInfo
{
    [JsonPropertyName("transactionCode")]
    public string TransactionCode { get; set; }

    [JsonPropertyName("productNumber")]
    public string ProductNumber { get; set; }

    [JsonPropertyName("distributionOrReceptionChannel")]
    public string DistributionOrReceptionChannel { get; set; }

    [JsonPropertyName("branchOffice")]
    public string BranchOffice { get; set; }

    [JsonPropertyName("city")]
    public string City { get; set; }

    [JsonPropertyName("country")]
    public string Country { get; set; }

    [JsonPropertyName("nature")]
    public string Nature { get; set; }

    [JsonPropertyName("transactionDate")]
    public string TransactionDate  { get; set; }

    [JsonPropertyName("operationValue")]
    public string OperationValue { get; set; }

    [JsonPropertyName("cashValue")]
    public string CashValue { get; set; }

    [JsonPropertyName("checkValue")]
    public string CheckValue { get; set; }

    [JsonPropertyName("electronicChannelValue")]
    public string ElectronicChannelValue { get; set; }

    [JsonPropertyName("currencyType")]
    public string CurrencyType { get; set; }
}