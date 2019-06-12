using System;
using Newtonsoft.Json;

namespace OrbitSample.Models
{
    public partial class Post
    {
        [JsonProperty("uniqueId")]
        public string UniqueId { get; set; }

        [JsonProperty("userId")]
        public string UserId { get; set; }

        [JsonProperty("title")]
        public string Title { get; set; }

        [JsonProperty("body")]
        public string Body { get; set; }

        [JsonIgnore]
        public string Id => $"{UserId}_{Title}";
    }
}