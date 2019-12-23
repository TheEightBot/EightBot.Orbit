using System;
namespace EightBot.Orbit.Client.SyncReconcilers
{
    public class ServerWinsSyncReconciler : ISyncReconciler
    {
        public T Reconcile<T>(ServerSyncInfo<T> server, ClientSyncInfo<T> client)
        {
            return server.Value;
        }
    }
}
