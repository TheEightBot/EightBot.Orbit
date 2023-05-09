using System;

namespace EightBot.Orbit.Client
{
    internal class Synchronizable<T>
    {
        public object TypeId { get; set; }

        public string TypeFullName { get; set; }

        public string Partition { get; set; }

        public long ModifiedTimestamp { get; set; }

        public long? SyncTimestamp { get; set; }

        public T Value { get; set; }

        public ClientOperationType Operation { get; set; }
    }
}
