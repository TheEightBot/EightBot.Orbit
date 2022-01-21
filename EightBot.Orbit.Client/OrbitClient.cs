using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Tycho;

namespace EightBot.Orbit.Client
{
    public class OrbitClient : IDisposable
    {
        private const string
            OrbitCacheDb = "OrbitCache.db",
            SyncCollection = "Synchronizable",

            SynchronizableTypeIdIndex = nameof(Synchronizable<object>.TypeId),
            SynchronizableModifiedTimestampIndex = nameof(Synchronizable<object>.ModifiedTimestamp),
            SynchronizableOperationIndex = nameof(Synchronizable<object>.Operation);

        private readonly object _scaffoldingLock = new object();

        private readonly ISyncReconciler _syncReconciler;

        private readonly ProcessingQueue _processingQueue = new ProcessingQueue();

        private TychoDb _db;
        private bool disposedValue;

        public static string PartitionSeparator { get; set; } = "___";

        public string CacheDirectory { get; private set; }

        public string CachePath { get; private set; }

        public bool Initialized { get; private set; }

        public OrbitClient(ISyncReconciler syncReconciler = null)
        {
            _syncReconciler = syncReconciler ?? new SyncReconcilers.ServerWinsSyncReconciler();
        }

        public OrbitClient Initialize(string cacheDirectory, string customCacheName = null, bool deleteExistingCache = false)
        {
            lock(_scaffoldingLock)
            {
                if(!Initialized)
                {
                    Initialized = true;

                    CacheDirectory = cacheDirectory;

                    CachePath = Path.Combine(cacheDirectory, customCacheName ?? OrbitCacheDb);

                    if(deleteExistingCache && File.Exists(CachePath))
                    {
                        File.Delete(CachePath);
                    }

                    _db =
                        new TychoDb(cacheDirectory, new NewtonsoftJsonSerializer(), rebuildCache: deleteExistingCache, requireTypeRegistration: true);

                    _db.Connect();

                    _db.AddTypeRegistration<Synchronizable<object>>();
                    _db.CreateIndex<Synchronizable<object>>(x => x.TypeId, "idxSynchronizableTypeId");
                    _db.CreateIndex<Synchronizable<object>>(x => x.ModifiedTimestamp, "idxSynchronizableModifiedTimestamp");
                    _db.CreateIndex<Synchronizable<object>>(x => x.Operation, "idxSynchronizableOperation");
                }
            }

            return this;
        }

        public OrbitClient Startup()
        {
            lock(_scaffoldingLock)
            {
                if (Initialized)
                {
                    return this;
                }

                _db.Connect();

                Initialized = true;

                return this;
            }
        }

        public void Shutdown()
        {
            lock(_scaffoldingLock)
            {
                if (!Initialized)
                    return;

                _db?.Disconnect();

                Initialized = false;
            }
        }

        public OrbitClient AddTypeRegistration<T, TId>(
            Expression<Func<T, object>> idSelector,
            EqualityComparer<TId> idComparer = null)
            where T : class
        {
            lock (_scaffoldingLock)
            {
                if (!Initialized)
                    throw new ClientNotInitializedException($"{nameof(Initialize)} must be called before you can add type registrations.");

                _db.AddTypeRegistration<T, TId>(idSelector, idComparer);
                _db.AddTypeRegistration<Synchronizable<T>, Guid>(x => x.Id);
            }

            return this;
        }

        public OrbitClient AddTypeRegistrationWithCustomKeySelector<T>(
            Func<T, string> idSelector,
            EqualityComparer<string> idComparer = null)
            where T : class
        {
            lock (_scaffoldingLock)
            {
                if (!Initialized)
                    throw new ClientNotInitializedException($"{nameof(Initialize)} must be called before you can add type registrations.");

                _db.AddTypeRegistrationWithCustomKeySelector<T>(idSelector, idComparer);
                _db.AddTypeRegistration<Synchronizable<T>, Guid>(x => x.Id);
            }

            return this;
        }

        public Task<(bool Success, ClientOperationType OperationResult)> Create<T>(T obj, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    var result = await ItemExistsAndAvailable(obj, partition).ConfigureAwait(false);

                    if (!result.IsDeleted && !result.Exists)
                    {
                        await _db
                            .WriteObjectAsync(GetAsSynchronizable(obj, ClientOperationType.Create), partition)
                            .ConfigureAwait(false);

                        return (true, ClientOperationType.Create);
                    }

                    return (false, ClientOperationType.NoOperation);
                });
        }

        public Task<(bool Success, ClientOperationType OperationResult)> Update<T>(T obj, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    var result = await ItemExistsAndAvailable(obj, partition).ConfigureAwait(false);

                    if (!result.IsDeleted && result.Exists)
                    {
                        await _db
                            .WriteObjectAsync(GetAsSynchronizable(obj, ClientOperationType.Update, partition), partition)
                            .ConfigureAwait(false);

                        return (true, ClientOperationType.Update);
                    }

                    return (false, ClientOperationType.NoOperation);
                });
        }

        public Task<(bool Success, ClientOperationType OperationResult)> Upsert<T>(T obj, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    var result = await ItemExistsAndAvailable(obj, partition).ConfigureAwait(false);

                    if (!result.IsDeleted && result.Exists)
                    {
                        await _db
                            .WriteObjectAsync(GetAsSynchronizable(obj, ClientOperationType.Update, partition), partition)
                            .ConfigureAwait(false);

                        return (true, ClientOperationType.Update);
                    }
                    else if (!result.IsDeleted)
                    {
                        await _db
                            .WriteObjectAsync(GetAsSynchronizable(obj, ClientOperationType.Create, partition), partition)
                            .ConfigureAwait(false);

                        return (true, ClientOperationType.Create);
                    }

                    return (false, ClientOperationType.NoOperation);
                });
        }

        public Task<(bool Success, ClientOperationType OperationResult)> Delete<T>(T obj, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    var result = await ItemExistsAndAvailable(obj, partition).ConfigureAwait(false);

                    if (!result.IsDeleted && result.Exists)
                    {
                        await _db.WriteObjectAsync(GetAsSynchronizable(obj, ClientOperationType.Delete, partition), partition);

                        return (true, ClientOperationType.Delete);
                    }

                    return (false, ClientOperationType.NoOperation);
                });
        }

        public Task<IEnumerable<T>> GetAllOf<T> (string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                () =>
                {
                    return _db.ReadObjectsAsync<T>(partition).AsTask();
                });
        }

        public async Task<IEnumerable<T>> GetAllLatest<T>(string partition = null)
            where T : class
        {
            var allOfType = new List<T>(await GetAllOf<T>(partition).ConfigureAwait(false));

            var latestSyncables = await GetAllLatestSyncQueue<T>(partition).ConfigureAwait(false);

            var rti = _db.GetRegisteredTypeInformationFor<T>();

            var matchedIndexes = new List<int>();

            for (int i = 0; i < latestSyncables.Count(); i++)
            {
                var latest = latestSyncables.ElementAt(i);

                var index = -1;

                for (int allOfTypeIndex = 0; allOfTypeIndex < allOfType.Count; allOfTypeIndex++)
                {
                    if(matchedIndexes.Contains(allOfTypeIndex))
                    {
                        continue;
                    }

                    if (rti.CompareIdsFor(latest, allOfType[allOfTypeIndex]))
                    {
                        matchedIndexes.Add(allOfTypeIndex);
                        index = allOfTypeIndex;
                        break;
                    }
                }

                if (index >= 0)
                {
                    allOfType[index] = latest;
                }
                else
                {
                    allOfType.Add(latest);
                }
            }

            return allOfType;
        }

        public async Task<T> GetLatest<T>(object key, string partition = null)
            where T : class
        {
            var syncQueueItem = await GetLatestSyncQueue<T>(key, partition).ConfigureAwait(false);

            if (syncQueueItem == null)
            {
                return await _db.ReadObjectAsync<T>(key, partition).ConfigureAwait(false);
            }

            if (syncQueueItem.Operation == ClientOperationType.Delete)
            {
                return default(T);
            }

            return syncQueueItem.Value;
        }

        public async Task<T> GetLatest<T>(T obj, string partition = null)
            where T : class
        {
            var syncQueueItem = await GetLatestSyncQueue(obj, partition).ConfigureAwait(false);

            if(syncQueueItem == null)
            {
                return await _db.ReadObjectAsync<T>(obj, partition).ConfigureAwait(false);
            }

            if(syncQueueItem.Operation == ClientOperationType.Delete)
            {
                return default(T);
            }

            return syncQueueItem.Value;
        }

        public Task<IEnumerable<T>> GetAllLatestSyncQueue<T>(string partition = null)
            where T: class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    return
                        (await _db.ReadObjectsAsync<Synchronizable<T>>(partition).ConfigureAwait(false))
                        ?.OrderByDescending(x => x.ModifiedTimestamp)
                        ?.GroupBy(x => x.TypeId)
                        ?.Where(x => !x.Any(i => i.Operation == ClientOperationType.Delete))
                        ?.Select(x => x.First().Value)
                        ?.ToArray()
                        ?? Enumerable.Empty<T>();
                });
        }

        public async Task<bool> PopulateCache<T>(IEnumerable<T> items, string partition = null, bool terminateSyncQueueHistory = false)
            where T : class
        {
            await DeleteCacheItems<T>(partition).ConfigureAwait(false);

            if (terminateSyncQueueHistory)
            {
                await TerminateSyncQueueHistory<T>(partition).ConfigureAwait(false);
            }

            return await _processingQueue
                .Queue(() => _db.WriteObjectsAsync(items, partition).AsTask())
                .ConfigureAwait(false);
        }

        public Task<bool> DeleteCacheItem<T>(T item, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                () =>
                {
                    var rti = _db.GetRegisteredTypeInformationFor<T>();

                    return _db.DeleteObjectAsync<T>(rti.GetIdFor<T>(item), partition).AsTask();
                });
        }

        public Task<bool> DeleteCacheItems<T>(string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    var result = await _db.DeleteObjectsAsync<T>(partition).ConfigureAwait(false);
                    return result >= 0;
                });
        }

        public Task<bool> UpsertCacheItem<T>(T item, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                () =>
                {
                    return _db.WriteObjectAsync(item, partition).AsTask();
                });
        }

        public Task<bool> UpsertCacheItems<T> (IEnumerable<T> items, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                () =>
                {
                    return _db.WriteObjectsAsync(items, partition).AsTask();
                });
        }

        public Task<IEnumerable<ClientSyncInfo<T>>> GetSyncHistory<T>(T obj, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    var items =
                        await _db
                            .ReadObjectsAsync<Synchronizable<T>>(
                                partition,
                                GetSynchronizableItemFilter<T>(obj))
                            .ConfigureAwait(false);

                    return items
                        ?.OrderByDescending(x => x.ModifiedTimestamp)
                        ?.Select(x => GetAsClientSyncInfo(x))
                        ?.ToArray()
                        ?? Enumerable.Empty<ClientSyncInfo<T>>();
                });
        }

        public Task<IEnumerable<ClientSyncInfo<T>>> GetSyncHistory<T>(SyncType syncType = SyncType.Latest, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    var items =
                        await _db
                            .ReadObjectsAsync(partition, GetSynchronizableItemFilter<T>())
                            .ConfigureAwait(false);

                    switch (syncType)
                    {
                        case SyncType.Latest:
                            return items
                                ?.OrderByDescending(x => x.ModifiedTimestamp)
                                ?.GroupBy(x => x.TypeId)
                                ?.Where(x => x?.Any() ?? false)
                                ?.Select(
                                    x =>
                                    {
                                        var latest = x.FirstOrDefault();
                                        return
                                            latest != default
                                                ? GetAsClientSyncInfo(latest)
                                                : default;
                                    })
                                ?.Where (x => x != default)
                                ?.ToArray()
                                ?? Enumerable.Empty<ClientSyncInfo<T>>();
                        case SyncType.FullHistory:
                            return items
                                ?.Where(x => x != default)
                                ?.OrderBy(x => x.ModifiedTimestamp)
                                ?.Select(x => GetAsClientSyncInfo(x))
                                ?.ToList()
                                ?? Enumerable.Empty<ClientSyncInfo<T>>();
                    }

                    return Enumerable.Empty<ClientSyncInfo<T>>();
                });
        }

        public async Task<bool> ReplaceSyncQueueHistory<T>(T obj, string partition = null)
            where T : class
        {
            if (!(await TerminateSyncQueueHistoryFor(obj, partition).ConfigureAwait(false)))
                return false;

            return (await Create(obj, partition).ConfigureAwait(false)).Success;
        }

        public Task<bool> TerminateSyncQueueHistoryFor<T>(T obj, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    var result =
                        await _db
                            .DeleteObjectsAsync(
                                partition,
                                GetSynchronizableItemFilter<T>(obj))
                            .ConfigureAwait(false);

                    return result >= 0;
                });
        }

        public Task<bool> TerminateSyncQueueHistoriesFor<T> (IEnumerable<T> objs, string partition = null)
            where T : class
        {
            var objsArray = objs as T[] ?? objs.ToArray();

            return _processingQueue.Queue(
                async () =>
                {
                    var deletions = 0;

                    foreach (var obj in objsArray)
                    {
                        var deleteResult =
                            await _db
                            .DeleteObjectsAsync(
                                partition,
                                GetSynchronizableItemFilter<T>(obj))
                            .ConfigureAwait(false);

                        deletions += deleteResult > 0 ? 1 : 0;
                    }

                    return deletions == objsArray.Length;
                });
        }

        public Task<bool> TerminateSyncQueueHistory<T>(string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    var result =
                        await _db
                            .DeleteObjectsAsync(
                                partition,
                                FilterBuilder<Synchronizable<T>>
                                    .Create()
                                    .Filter(FilterType.Equals, x => x.Partition, partition))
                            .ConfigureAwait(false);

                    return result >= 0;
                });
        }

        public Task<bool> TerminateSyncQueueHistoryAt<T>(T obj, DateTimeOffset offset, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    var result =
                        await _db
                            .DeleteObjectsAsync(
                                partition,
                                GetSynchronizableItemFilter<T>(obj)
                                    .And()
                                    .Filter(FilterType.Equals, x => x.ModifiedTimestamp, offset.ToUnixTimeMilliseconds()))
                            .ConfigureAwait(false);

                    return result >= 0;
                });
        }

        public Task<bool> TerminateSyncQueueHistoryBefore<T>(T obj, DateTimeOffset offset, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    var result =
                        await _db
                            .DeleteObjectsAsync(
                                partition,
                                GetSynchronizableItemFilter<T>(obj)
                                    .And()
                                    .Filter(FilterType.LessThan, x => x.ModifiedTimestamp, offset.ToUnixTimeMilliseconds()))
                            .ConfigureAwait(false);

                    return result >= 0;
                });
        }

        public Task<bool> TerminateSyncQueueHistoryAfter<T>(T obj, DateTimeOffset offset, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    var result =
                        await _db
                            .DeleteObjectsAsync(
                                partition,
                                GetSynchronizableItemFilter<T>(obj)
                                    .And()
                                    .Filter(FilterType.GreaterThan, x => x.ModifiedTimestamp, offset.ToUnixTimeMilliseconds()))
                            .ConfigureAwait(false);

                    return result >= 0;
                });
        }

        public async Task Reconcile<T> (IEnumerable<ServerSyncInfo<T>> serverSyncInformation, string partition = null)
            where T : class
        {
            var latest = await GetSyncHistory<T> (SyncType.Latest, partition).ConfigureAwait (false);

            var replacements = new List<T> ();
            var inserts = new List<T> ();

            var rti = _db.GetRegisteredTypeInformationFor<T>();

            if (latest.Any ())
            {
                foreach (var serverSyncInfo in serverSyncInformation)
                {
                    var latestClientUpdate = latest.FirstOrDefault (x => rti.CompareIdsFor(serverSyncInfo.Value, x.Value));

                    if (latestClientUpdate != null)
                    {
                        replacements.Add (_syncReconciler.Reconcile (serverSyncInfo, latestClientUpdate));
                        continue;
                    }

                    inserts.Add (serverSyncInfo.Value);
                }
            }
            else
            {
                inserts.AddRange (serverSyncInformation.Select (x => x.Value));
            }

            if (replacements.Any ())
            {
                await TerminateSyncQueueHistoriesFor(replacements, partition).ConfigureAwait (false);
                await UpsertCacheItems (replacements, partition).ConfigureAwait (false);
            }

            if (inserts.Any ())
            {
                await UpsertCacheItems (inserts, partition).ConfigureAwait (false);
            }
        }

        private async Task<(bool IsDeleted, bool Exists)> ItemExistsAndAvailable<T>(T obj, string partition = null)
            where T : class
        {
            var synchObjects =
                await _db
                    .ReadObjectsAsync<Synchronizable<T>>(
                        partition,
                        GetSynchronizableItemFilter<T>(obj))
                    .ConfigureAwait(false);

            var latestObject = synchObjects?.OrderByDescending(x => x.ModifiedTimestamp)?.FirstOrDefault();

            if(latestObject != null)
            {
                return (latestObject?.Operation == ClientOperationType.Delete, true);
            }

            var cachedObject =
                await _db
                    .ReadObjectAsync<T>(obj, partition)
                    .ConfigureAwait(false);

            return (false, cachedObject != null);
        }

        private Task<Synchronizable<T>> GetLatestSyncQueue<T>(T obj, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    var synchObjects =
                        await _db
                            .ReadObjectsAsync<Synchronizable<T>>(
                                partition,
                                GetSynchronizableItemFilter<T>(obj))
                            .ConfigureAwait(false);

                    return synchObjects?.OrderByDescending(x => x.ModifiedTimestamp)?.FirstOrDefault();
                });
        }

        private Task<Synchronizable<T>> GetLatestSyncQueue<T>(object key, string partition = null)
            where T : class
        {
            return _processingQueue.Queue(
                async () =>
                {
                    var synchObjects =
                        await _db
                            .ReadObjectsAsync<Synchronizable<T>>(
                                partition,
                                GetSynchronizableItemFilter<T>(key))
                            .ConfigureAwait(false);

                    return synchObjects?.OrderByDescending(x => x.ModifiedTimestamp)?.FirstOrDefault();
                });
        }

        private Synchronizable<T> GetAsSynchronizable<T>(T obj, ClientOperationType operationType, string partition = null)
            where T : class
        {
            var rti = _db.GetRegisteredTypeInformationFor<T>();

            var now = DateTimeOffset.Now;

            return new Synchronizable<T>
            {
                Id = Guid.NewGuid(),
                TypeId = rti.GetIdFor(obj),
                Partition = partition,
                TypeFullName = rti.TypeFullName,
                Value = obj,
                ModifiedTimestamp = now.ToUnixTimeMilliseconds(),
                Operation = operationType
            };
        }

        private FilterBuilder<Synchronizable<T>> GetSynchronizableItemFilter<T>()
        {
            var rti = _db.GetRegisteredTypeInformationFor<T>();

            var fb = FilterBuilder<Synchronizable<T>>
                .Create();

            return fb;
        }

        private FilterBuilder<Synchronizable<T>> GetSynchronizableItemFilter<T>(T obj)
        {
            var rti = _db.GetRegisteredTypeInformationFor<T>();

            var fb = FilterBuilder<Synchronizable<T>>
                .Create()
                .Filter(FilterType.Equals, x => x.TypeId, rti.GetIdFor(obj));

            return fb;
        }

        private FilterBuilder<Synchronizable<T>> GetSynchronizableItemFilter<T>(object key)
        {
            var rti = _db.GetRegisteredTypeInformationFor<T>();

            var fb = FilterBuilder<Synchronizable<T>>
                .Create()
                .Filter(FilterType.Equals, x => x.TypeId, key);

            return fb;
        }

        private ClientSyncInfo<T> GetAsClientSyncInfo<T>(Synchronizable<T> synchronizable)
        {
            return new ClientSyncInfo<T>
            {
                ModifiedOn = synchronizable.ModifiedTimestamp,
                Operation = synchronizable.Operation,
                Partition = synchronizable.Partition,
                Value = synchronizable.Value
            };
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _db?.Disconnect();
                    _db?.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
