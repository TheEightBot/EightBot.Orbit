using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EightBot.Orbit.Server
{
    public interface IOrbitDataClient
    {
        Task<IEnumerable<ServerSyncInfo<T>>> Sync<T>(IEnumerable<ClientSyncInfo<T>> syncables);
    }
}