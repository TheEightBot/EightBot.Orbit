using EightBot.Nebula.DocumentDb;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace EightBot.Orbit.Server.Data
{
    public class OrbitCosmosDataClient : IOrbitDataClient
    {
        private readonly IDataClient DataClient = null;



        public OrbitCosmosDataClient(IDataClient dataClient)
        {
            this.DataClient = dataClient;
        }

        public async Task<IEnumerable<ServerSyncInfo<T>>> Sync<T>(IEnumerable<ClientSyncInfo<T>> syncables)
        {
            var payload = new List<ServerSyncInfo<T>>();

            if (syncables != null && syncables.Count() > 0)
            {
                for (var i = 0; i < syncables.Count(); i++)
                {
                    var syncable = syncables.ElementAt(i);

                    var id = this.DataClient.GetId(syncable.Value);
                    var isGuid = Guid.TryParse(id, out var idGuid);

                    var partitionKey = this.DataClient.GetPartitionKey(syncable.Value);

                    var syncableLastModified = DateTimeOffset.FromUnixTimeMilliseconds(syncable.ModifiedOn).UtcDateTime;

                    if (!isGuid || (isGuid && idGuid != Guid.Empty))
                    {
                        if (syncable.Operation == ClientOperationType.Create || syncable.Operation == ClientOperationType.Update)
                        {
                            var existingDocumentWithBase = await this.DataClient.Document<T>().GetWithBaseAsync(id, partitionKey).ConfigureAwait(false);
                            if (existingDocumentWithBase.Document == null)
                            {
                               var pk =  this.DataClient.GetPartitionKey<T>(syncable.Value);

                                var success = await this.DataClient.Document<T>().UpsertAsync(syncable.Value).ConfigureAwait(false);
                                if (success)
                                    await AddToPayload(id, partitionKey, ServerOperationType.Created, DateTime.UtcNow, default(T), payload);
                            }
                            else
                            {
                                var serverLastModified = existingDocumentWithBase.BaseDocument.Timestamp;

                                // Server Wins!
                                if (serverLastModified > syncableLastModified)
                                    await AddToPayload(id, partitionKey, ServerOperationType.Updated, serverLastModified, existingDocumentWithBase.Document, payload);
                                else
                                {
                                    var success = await this.DataClient.Document<T>().UpsertAsync(syncable.Value).ConfigureAwait(false);
                                    await AddToPayload(id, partitionKey, ServerOperationType.Updated, DateTime.UtcNow, default(T), payload);
                                }
                            }
                        }
                        else if (syncables.ElementAt(i).Operation == ClientOperationType.Delete)
                        {
                            var succes = await this.DataClient.Document<T>().DeleteAsync(id, partitionKey).ConfigureAwait(false);
                            if (succes)
                                await AddToPayload(id, partitionKey, ServerOperationType.Deleted, DateTime.UtcNow, default(T), payload);
                            else
                                await AddToPayload(id, partitionKey, ServerOperationType.NotModified, syncableLastModified, default(T), payload);
                        }
                    }
                    else
                        await AddToPayload(id, partitionKey, ServerOperationType.NotModified, syncableLastModified, default(T), payload);
                }
            }

            return payload;
        }

        private async Task AddToPayload<T>(string id, object partitionKey, ServerOperationType operation, DateTime modified, T existingDocument, List<ServerSyncInfo<T>> payload)
        {
            var payloadItem = new ServerSyncInfo<T>() { Id = id, Operation = operation };

            if (existingDocument == null && payloadItem.Operation == ServerOperationType.Created || payloadItem.Operation == ServerOperationType.Updated)
            {
                var existingDocumentWithBase = await this.DataClient.Document<T>().GetWithBaseAsync(id, partitionKey).ConfigureAwait(false);
                if (existingDocumentWithBase.BaseDocument != null)
                    modified = existingDocumentWithBase.BaseDocument.Timestamp;

                if (existingDocumentWithBase.Document != null)
                    existingDocument = existingDocumentWithBase.Document;
            }

            payloadItem.ModifiedOn = new DateTimeOffset(modified).ToUnixTimeMilliseconds();
            payloadItem.Value = existingDocument;

            payload.Add(payloadItem);
        }
    }
}