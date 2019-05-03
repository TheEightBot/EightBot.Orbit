using System;

namespace EightBot.Orbit
{
    public class SyncInfo<T>
    {
        public long ModifiedOn { get; set; }

        public OperationType Operation { get; set; }

        public T Value { get; set; }
    }
}
