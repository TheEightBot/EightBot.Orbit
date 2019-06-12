using System;
namespace EightBot.Orbit.Client.SyncReconcilers
{
    public class ServerWinsSyncReconciler : ISyncReconciler
    {
        public SyncReconciliationAction Reconcile<T>(ServerSyncInfo<T> server, ClientSyncInfo<T> client)
        {
            switch (server.Operation)
            {
                case ServerOperationType.Created:
                    return SyncReconciliationAction.ReplaceWithServerValue;
                case ServerOperationType.Deleted:
                    return SyncReconciliationAction.RemoveClientValue;
                default:
                    return SyncReconciliationAction.KeepClientValue;
            }
        }
    }
}
