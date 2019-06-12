using System;

namespace EightBot.Orbit
{
    public class ClientSyncInfo<T>
    {
        public long ModifiedOn { get; set; }

        public ClientOperationType Operation { get; set; }

        public string Category { get; set; }

        public T Value { get; set; }
    }
}