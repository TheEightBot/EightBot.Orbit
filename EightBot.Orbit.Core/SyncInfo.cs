using System;

namespace EightBot.Orbit
{
    public class SyncInfo<T>
    {
        public long ModifiedOn { get; set; }

        public OperationType Operation { get; set; }

        public string Category { get; set; }

        public T Value { get; set; }
    }

    public class SyncedInfo<T>
    {
        public OperationType Operation { get; set; }

        public string Id { get; set; }

        public T Value { get; set; }
    }
}