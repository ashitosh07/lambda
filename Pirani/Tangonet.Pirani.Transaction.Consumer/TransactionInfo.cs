using System.Text.Json.Serialization;

namespace Tangonet_Pirani_Transaction_Consumer
{
    public partial class Function
    {
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
            public DateTime TransactionDate { get; set; }

            [JsonPropertyName("operationValue")]
            public decimal OperationValue { get; set; }

            [JsonPropertyName("cashValue")]
            public decimal CashValue { get; set; }

            [JsonPropertyName("checkValue")]
            public decimal CheckValue { get; set; }

            [JsonPropertyName("electronicChannelValue")]
            public decimal ElectronicChannelValue { get; set; }

            [JsonPropertyName("currencyType")]
            public string CurrencyType { get; set; }
        }
    }


}
