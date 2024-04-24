using System.Text.Json.Serialization;

namespace dev_tangonet_product_producer_lambda
{
    public partial class Function
    {
        public class ProductInfo
        {
            [JsonPropertyName("product")]
            public string Product { get; set; }

            [JsonPropertyName("subproduct")]
            public string Subproduct { get; set; }

            [JsonPropertyName("productState")]
            public string ProductState { get; set; }

            [JsonPropertyName("productNumber")]
            public string ProductNumber { get; set; }

            [JsonPropertyName("registrationDate")]
            public string RegistrationDate { get; set; }

            [JsonPropertyName("city")]
            public string City { get; set; }

            [JsonPropertyName("branchOffice")]
            public string BranchOffice { get; set; }

            [JsonPropertyName("distributionChannel")]
            public string DistributionChannel { get; set; }

            [JsonPropertyName("identificationType")]
            public string IdentificationType { get; set; }

            [JsonPropertyName("identificationNumber")]
            public string IdentificationNumber { get; set; }

            [JsonPropertyName("parentType")]
            public string ParentType { get; set; }

        }
    }

}