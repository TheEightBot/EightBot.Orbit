using EightBot.Nebula.DocumentDb;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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

        public async Task<IEnumerable<T>> Sync<T>(IEnumerable<SyncInfo<T>> syncables)
        {
            var payload = new List<T>();

            if (syncables != null && syncables.Count() > 0)
            {
                var documentIdentifiers = new Dictionary<string, string>();
                for (var i = 0; i < syncables.Count(); i++)
                {
                    var syncable = syncables.ElementAt(i);

                    var id = this.DataClient.GetId(syncable);
                    var partitionKey = this.DataClient.GetPartitionKey(syncable);
                    if (!String.IsNullOrWhiteSpace(id) && new Guid(id) != Guid.Empty)
                    {
                        if (syncable.Operation == OperationType.Create || syncable.Operation == OperationType.Update)
                        {
                            var existingDocument = await this.DataClient.Document<T>().GetAsync(id, partitionKey).ConfigureAwait(false);
                            if (existingDocument == null)
                                documentIdentifiers.Add(await this.DataClient.Document<T>().UpsertAsync(syncable.Value).ConfigureAwait(false), partitionKey);
                            else
                            {
                                var syncableLastModified = DateTimeOffset.FromUnixTimeMilliseconds(syncable.ModifiedOn).UtcDateTime;
                                var serverLastModified = DateTime.UtcNow;

                                var documentResponse = await this.DataClient.Client.ReadDocumentAsync(UriFactory.CreateDocumentUri(this.DataClient.DatabaseId, typeof(T).Name.Pluralize(), id), new RequestOptions() { PartitionKey = new PartitionKey(partitionKey) }).ConfigureAwait(false);
                                if (documentResponse != null && documentResponse.Resource != null)
                                    serverLastModified = documentResponse.Resource.Timestamp;

                                // Server Wins!
                                if (serverLastModified > syncableLastModified)
                                    payload.Add(existingDocument);
                                else
                                    documentIdentifiers.Add(await this.DataClient.Document<T>().UpsertAsync(syncable.Value).ConfigureAwait(false), partitionKey);
                            }
                        }
                        else if (syncables.ElementAt(i).Operation == OperationType.Delete)
                        {
                            var succes = await this.DataClient.Document<T>().DeleteAsync(id, partitionKey).ConfigureAwait(false);
                            if (!succes)
                                payload.Add(syncable.Value);
                        }
                    }
                }

                foreach (KeyValuePair<string, string> entry in documentIdentifiers)
                {
                    var existingDocument = await this.DataClient.Document<T>().GetAsync(entry.Key, entry.Value).ConfigureAwait(false);
                    if (existingDocument != null)
                        payload.Add(existingDocument);
                }
            }

            return payload;
        }
    }
}