using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EightBot.Orbit.Server
{
    public interface IOrbitDataClient
    {
        //void AddTypeRegistration<T, TId, TPk>(Expression<Func<T, TId>> idSelector, Expression<Func<T, TPk>> partitionKeySelector);

        Task<IEnumerable<T>> Sync<T>(IEnumerable<SyncInfo<T>> syncables);
    }
}