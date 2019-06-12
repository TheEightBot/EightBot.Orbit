using EightBot.Nebula.DocumentDb;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace EightBot.Orbit.Server.Data
{
    public class OrbitCosmosClient : IOrbitDataClient
    {
        private readonly IDataClient DataClient = null;

        public OrbitCosmosClient(IDataClient dataClient)
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

                    if (!isGuid || (isGuid && idGuid != Guid.Empty))
                    {
                        if (syncable.Operation == ClientOperationType.Create || syncable.Operation == ClientOperationType.Update)
                        {
                            var existingDocument = await this.DataClient.Document<T>().GetWithBaseAsync(id, partitionKey).ConfigureAwait(false);
                            if (existingDocument.Document == null)
                                await AddToPayload(await this.DataClient.Document<T>().UpsertAsync(syncable.Value).ConfigureAwait(false), partitionKey, ServerOperationType.Created, default(T), payload);
                            else
                            {
                                var syncableLastModified = DateTimeOffset.FromUnixTimeMilliseconds(syncable.ModifiedOn).UtcDateTime;
                                var serverLastModified = existingDocument.BaseDocument.Timestamp;

                                // Server Wins!
                                if (serverLastModified > syncableLastModified)
                                    await AddToPayload(id, partitionKey, ServerOperationType.Updated, existingDocument.Document, payload);
                                else
                                    await AddToPayload(await this.DataClient.Document<T>().UpsertAsync(syncable.Value).ConfigureAwait(false), partitionKey, ServerOperationType.Updated, default(T), payload);
                            }
                        }
                        else if (syncables.ElementAt(i).Operation == ClientOperationType.Delete)
                        {
                            var succes = await this.DataClient.Document<T>().DeleteAsync(id, partitionKey).ConfigureAwait(false);
                            if (succes)
                                await AddToPayload(id, partitionKey, ServerOperationType.Deleted, default(T), payload);
                            else
                                await AddToPayload(id, partitionKey, ServerOperationType.NotModified, default(T), payload);
                        }
                    }
                    else
                        await AddToPayload(id, partitionKey, ServerOperationType.NotModified, default(T), payload);
                }
            }

            return payload;
        }

        private async Task AddToPayload<T>(string id, object partitionKey, ServerOperationType operation, T existingDocument, List<ServerSyncInfo<T>> payload)
        {
            var payloadItem = new ServerSyncInfo<T>() { Id = id, Operation = operation };

            if (existingDocument == null && payloadItem.Operation == ServerOperationType.Created || payloadItem.Operation == ServerOperationType.Updated)
            {
                existingDocument = await this.DataClient.Document<T>().GetAsync(id, partitionKey).ConfigureAwait(false);
                if (existingDocument != null)
                    payloadItem.Value = existingDocument;
            }

            payloadItem.Value = existingDocument;

            payload.Add(payloadItem);
        }
    }
}