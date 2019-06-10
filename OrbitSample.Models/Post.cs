using System;
using Newtonsoft.Json;

namespace OrbitSample.Models
{
    public partial class Post
    {
        //[JsonProperty("id")]
        //public string Id { get; set; }

        [JsonProperty("userId")]
        public string UserId { get; set; }

        [JsonProperty("title")]
        public string Title { get; set; }

        [JsonProperty("body")]
        public string Body { get; set; }
    }
}