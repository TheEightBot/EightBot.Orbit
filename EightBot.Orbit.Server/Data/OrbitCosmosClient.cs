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

        public async Task<IEnumerable<SyncedInfo<T>>> Sync<T>(IEnumerable<SyncInfo<T>> syncables)
        {
            var payload = new List<SyncedInfo<T>>();

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
                        if (syncable.Operation == OperationType.Create || syncable.Operation == OperationType.Update)
                        {
                            var existingDocument = await this.DataClient.Document<T>().GetWithBaseAsync(id, partitionKey).ConfigureAwait(false);
                            if (existingDocument.Document == null)
                                await AddToPayload(await this.DataClient.Document<T>().UpsertAsync(syncable.Value).ConfigureAwait(false), partitionKey, OperationType.Created, default(T), payload);
                            else
                            {
                                var syncableLastModified = DateTimeOffset.FromUnixTimeMilliseconds(syncable.ModifiedOn).UtcDateTime;
                                var serverLastModified = existingDocument.BaseDocument.Timestamp;

                                // Server Wins!
                                if (serverLastModified > syncableLastModified)
                                    await AddToPayload(id, partitionKey, OperationType.Updated, existingDocument.Document, payload);
                                else
                                    await AddToPayload(await this.DataClient.Document<T>().UpsertAsync(syncable.Value).ConfigureAwait(false), partitionKey, OperationType.Updated, default(T), payload);
                            }
                        }
                        else if (syncables.ElementAt(i).Operation == OperationType.Delete)
                        {
                            var succes = await this.DataClient.Document<T>().DeleteAsync(id, partitionKey).ConfigureAwait(false);
                            if (succes)
                                await AddToPayload(id, partitionKey, OperationType.Deleted, default(T), payload);
                            else
                                await AddToPayload(id, partitionKey, OperationType.NotModified, default(T), payload);
                        }
                    }
                    else
                        await AddToPayload(id, partitionKey, OperationType.NotModified, default(T), payload);
                }
            }

            return payload;
        }

        private async Task AddToPayload<T>(string id, object partitionKey, OperationType operation, T existingDocument, List<SyncedInfo<T>> payload)
        {
            var payloadItem = new SyncedInfo<T>() { Id = id, Operation = operation };

            if (existingDocument == null && payloadItem.Operation == OperationType.Created || payloadItem.Operation == OperationType.Updated)
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