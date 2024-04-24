using System.Text.Json.Serialization;

namespace Tangonet_Pirani_Transaction_Consumer
{
    public partial class Function
    {
        public class TransactionInfo
        {
            public string Id { get; set; }
            public string UserId { get; set; }

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
            public string OperationValue { get; set; }

            [JsonPropertyName("currencyType")]
            public string CurrencyType { get; set; }




            [JsonPropertyName("transactionType")]
            public string TransactionType { get; set; }

            [JsonPropertyName("TransactionFee")]
            public string TransactionFee { get; set; }

            [JsonPropertyName("terminalId")]
            public string TerminalId { get; set; }

            [JsonPropertyName("partnerId")]
            public string PartnerId { get; set; }

            [JsonPropertyName("transactionStatus")]
            public string TransactionStatus { get; set; }

            [JsonPropertyName("senderCurrency")]
            public string SenderCurrency { get; set; }

            [JsonPropertyName("senderFees")]
            public string SenderFees { get; set; }

            [JsonPropertyName("deliveryOption")]
            public string DeliveryOption { get; set; }

            [JsonPropertyName("deliveryCountry")]
            public string DeliveryCountry { get; set; }

            [JsonPropertyName("ReceiverFirstName")]
            public string ReceiverFirstName { get; set; }

            [JsonPropertyName("receiverLastName")]
            public string ReceiverLastName { get; set; }

            [JsonPropertyName("receiveCurrency")]
            public string ReceiveCurrency { get; set; }

            [JsonPropertyName("receiveAmount")]
            public string ReceiveAmount { get; set; }
        }


    }


}
