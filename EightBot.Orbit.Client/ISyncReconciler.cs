using System;
namespace EightBot.Orbit.Client
{
    public interface ISyncReconciler
    {
        T Reconcile<T>(ServerSyncInfo<T> server, ClientSyncInfo<T> client);
    }
}
