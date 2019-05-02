using System;
using System.ComponentModel;
namespace EightBot.Orbit.Core
{
    public abstract class SyncBase<T>
    {
        public Guid Id { get; set; }

        public DateTimeOffset ModifiedDate { get; set; }

        public virtual bool Equals(SyncBase<T> other)
        {
            return Id == other.Id
                && ModifiedDate == other.ModifiedDate;
        }
    }
}
