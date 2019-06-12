using System;
namespace EightBot.Orbit
{
    public class ServerSyncInfo<T>
    {
        public ServerOperationType Operation { get; set; }

        public string Id { get; set; }

        public long ModifiedOn { get; set; }

        public T Value { get; set; }
    }
}
