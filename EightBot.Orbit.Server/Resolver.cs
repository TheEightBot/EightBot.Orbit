using System;

namespace EightBot.Orbit.Server
{
    public abstract class Resolver
    {
        public T Resolve<T>(ClientSyncInfo<T> clientModel, T serverModel)
        {
            return default(T);
        }
    }
}