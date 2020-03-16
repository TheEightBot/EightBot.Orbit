using System;
using LiteDB;
namespace EightBot.Orbit.Client
{
    internal class Synchronizable<T>
    {
        public ObjectId Id { get; set; }

        public string Category { get; set; }

        public string TypeName { get; set; }

        public long ModifiedTimestamp { get; set; }

        public long? SyncTimestamp { get; set; }

        public BsonValue TypeId { get; set; }

        public T Value { get; set; }

        public int Operation { get; set; }
    }
}
