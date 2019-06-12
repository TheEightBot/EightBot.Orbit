using System;
namespace EightBot.Orbit.Client
{
    public interface ISyncReconciler
    {
        SyncReconciliationAction Reconcile<T>(ServerSyncInfo<T> server, ClientSyncInfo<T> client);
    }

    public enum SyncReconciliationAction
    {
        None,
        ReplaceWithServerValue,
        KeepClientValue,
        RemoveClientValue
    }
}
