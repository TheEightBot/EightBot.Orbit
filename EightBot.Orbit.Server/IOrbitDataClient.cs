using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EightBot.Orbit.Server
{
    public interface IOrbitDataClient
    {
        Task<IEnumerable<SyncedInfo<T>>> Sync<T>(IEnumerable<SyncInfo<T>> syncables);
    }
}