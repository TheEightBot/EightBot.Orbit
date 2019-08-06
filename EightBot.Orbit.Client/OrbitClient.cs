using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using LiteDB;
using System.Threading.Tasks.Dataflow;
using System.Threading.Tasks;

namespace EightBot.Orbit.Client
{
    public class OrbitClient
    {
        private const string
            OrbitCacheDb = "OrbitCache.db",
            SyncCollection = "Synchronizable",

            SynchronizableTypeIdIndex = nameof(Synchronizable<object>.TypeId),
            SynchronizableCategory = nameof(Synchronizable<object>.Category),
            SynchronizableTypeNameIndex = nameof(Synchronizable<object>.TypeName),
            SynchronizableModifiedTimestampIndex = nameof(Synchronizable<object>.ModifiedTimestamp),
            SynchronizableOperationIndex = nameof(Synchronizable<object>.Operation);

        private readonly object _scaffoldingLock = new object();

        private readonly ISyncReconciler _syncReconciler;

        private readonly Dictionary<Type, RegisteredTypeInformation> _registeredTypes =
            new Dictionary<Type, RegisteredTypeInformation>();

        private readonly ProcessingQueue _processingQueue = new ProcessingQueue();

        private LiteDatabase _db;

        private string _additionalConnectionStringParameters;

        public static string CategorySeparator { get; set; } = "_-_";

        public string CachePath { get; private set; }

        public bool Initialized { get; private set; }

        public OrbitClient(ISyncReconciler syncReconciler = null)
        {
            _syncReconciler = syncReconciler ?? new SyncReconcilers.ServerWinsSyncReconciler();
        }

        public OrbitClient Initialize(string cacheDirectory, string customCacheName = null, string additionalConnectionStringParameters = null)
        {
            lock(_scaffoldingLock)
            {
                if(!Initialized)
                {
                    Initialized = true;

                    CachePath = Path.Combine(cacheDirectory, customCacheName ?? OrbitCacheDb);

                    _additionalConnectionStringParameters = additionalConnectionStringParameters;

                    _db = new LiteDatabase($"Filename={CachePath};{additionalConnectionStringParameters}");

                    var syncCollection = _db.GetCollection(SyncCollection);

                    syncCollection.EnsureIndex(SynchronizableTypeIdIndex);
                    syncCollection.EnsureIndex(SynchronizableCategory);
                    syncCollection.EnsureIndex(SynchronizableTypeNameIndex);
                    syncCollection.EnsureIndex(SynchronizableModifiedTimestampIndex);
                    syncCollection.EnsureIndex(SynchronizableOperationIndex);
                }
            }

            return this;
        }

        public OrbitClient Startup()
        {
            lock(_scaffoldingLock)
            {
                if (Initialized && _db == null)
                {
                    _db = new LiteDatabase($"Filename={CachePath};{_additionalConnectionStringParameters}");
                }
            }

            return this;
        }

        public void Shutdown()
        {
            lock(_scaffoldingLock)
            {
                if (!Initialized || _db == null)
                    return;

                _db?.Dispose();
                _db = null;

                Initialized = false;
            }
        }

        public OrbitClient AddTypeRegistration<T, TId>(Expression<Func<T, TId>> idSelector, string typeNameOverride = null)
            where T : class
        {
            lock(_scaffoldingLock)
            {
                if (!Initialized)
                    throw new ClientNotInitializedException($"{nameof(Initialize)} must be called before you can add type registrations.");

                var rti = RegisteredTypeInformation.Create(idSelector, typeNameOverride);

                _registeredTypes[rti.ObjectType] = rti;

                _db.Mapper
                    .Entity<T>()
                    .Id(idSelector, false);
            }


            return this;
        }

        public OrbitClient AddTypeRegistration<T, TId>(Expression<Func<T, string>> idSelector, Expression<Func<T, TId>> idProperty, string typeNameOverride = null)
            where T : class
        {
            lock(_scaffoldingLock)
            {
                if (!Initialized)
                    throw new ClientNotInitializedException($"{nameof(Initialize)} must be called before you can add type registrations.");

                var rti = RegisteredTypeInformation.Create(idSelector, idProperty, typeNameOverride);

                _registeredTypes[rti.ObjectType] = rti;

                _db.Mapper
                    .Entity<T>()
                    .Id(idProperty, false);
            }

            return this;
        }

        public Task<bool> Create<T>(T obj, string category = null)
            where T : class
        {
            return _processingQueue.Queue(
                () => {
                    var syncCollection = GetSynchronizableTypeCollection<T>();

                    if (!syncCollection.Exists(GetItemQuery(obj, category)))
                    {
                        syncCollection.Insert(GetAsSynchronizable(obj, ClientOperationType.Create, category));

                        return true;
                    }

                    return false;
                });
        }

        public Task<bool> Update<T>(T obj, string category = null)
            where T : class
        {
            return 
                _processingQueue
                    .Queue(
                        () =>
                        {
                            if (ItemExistsAndAvailable(obj, category))
                            {
                                var syncCollection = GetSynchronizableTypeCollection<T>();
                                syncCollection.Insert(GetAsSynchronizable(obj, ClientOperationType.Update, category));
                                return true;
                            }

                            return false;
                        });
        }

        public Task<bool> Upsert<T>(T obj, string category = null)
            where T : class
        {
            return
                _processingQueue
                    .Queue(
                        () =>
                        {
                            var syncCollection = GetSynchronizableTypeCollection<T>();
                            if (ItemExistsAndAvailable(obj, category))
                            {
                                syncCollection.Insert(GetAsSynchronizable(obj, ClientOperationType.Update, category));
                            }

                            syncCollection.Insert(GetAsSynchronizable(obj, ClientOperationType.Create, category));

                            return true;
                        });
        }

        public Task<bool> Delete<T>(T obj, string category = null)
            where T : class
        {
            return
                _processingQueue
                    .Queue(
                        () =>
                        {
                            if (ItemExistsAndAvailable(obj, category))
                            {
                                var syncCollection = GetSynchronizableTypeCollection<T>();
                                syncCollection.Insert(GetAsSynchronizable(obj, ClientOperationType.Delete, category));
                                return true;
                            }

                            return false;
                        });
        }

        public async Task<bool> ReplaceSyncQueueHistory<T>(T obj, string category = null)
            where T : class
        {
            if (!(await TerminateSyncQueueHisory(obj, category).ConfigureAwait(false)))
                return false;

            return await Create(obj, category).ConfigureAwait(false);
        }

        public Task<bool> TerminateSyncQueueHisory<T>(T obj, string category = null)
            where T : class
        {
            return _processingQueue.Queue(
                () =>
                {
                    var syncCollection = GetSynchronizableTypeCollection<T>();

                    return syncCollection.Delete(GetItemQuery(obj, category)) > 0;
                });
        }

        public Task<bool> TerminateSyncQueueHisory<T>(string category = null)
            where T : class
        {
            return _processingQueue.Queue(
                () =>
                {
                    var syncCollection = GetSynchronizableTypeCollection<T>();

                    return syncCollection.Delete(GetItemQuery<T>(category)) > 0;
                });
        }

        public Task<bool> TerminateSyncQueueHistoryAt<T>(T obj, DateTimeOffset offset, string category = null)
            where T : class
        {
            return _processingQueue.Queue(
                () =>
                {
                    var syncCollection = GetSynchronizableTypeCollection<T>();

                    return syncCollection
                        .Delete(
                            Query.And(
                                GetItemQuery(obj, category),
                                Query.EQ(SynchronizableModifiedTimestampIndex, offset.ToUnixTimeMilliseconds()))) > 0;
                });
        }

        public Task<bool> TerminateSyncQueueHistoryBefore<T>(T obj, DateTimeOffset offset, string category = null)
            where T : class
        {
            return _processingQueue.Queue(
                () =>
                {
                    var syncCollection = GetSynchronizableTypeCollection<T>();

                    return syncCollection
                        .Delete(
                            Query.And(
                                GetItemQuery(obj, category),
                                Query.LT(SynchronizableModifiedTimestampIndex, offset.ToUnixTimeMilliseconds()))) > 0;
                });
        }

        public Task<bool> TerminateSyncQueueHistoryAfter<T>(T obj, DateTimeOffset offset, string category = null)
            where T : class
        {
            return _processingQueue.Queue(
                () =>
                {
                    var syncCollection = GetSynchronizableTypeCollection<T>();

                    return syncCollection
                        .Delete(
                            Query.And(
                                GetItemQuery(obj, category),
                                Query.GT(SynchronizableModifiedTimestampIndex, offset.ToUnixTimeMilliseconds()))) > 0;
                });
        }

        public async Task<IEnumerable<string>> GetCategories<T>()
            where T : class
        {
            var rti = _registeredTypes[typeof(T)];
            var ctn = rti.GetCategoryTypeName();

            var typeCollectionNames =
                await _processingQueue
                    .Queue(
                        () =>
                        {
                            return _db.GetCollectionNames()
                                    ?.Where(x => x.StartsWith(ctn, StringComparison.Ordinal) && x.Contains(CategorySeparator))
                                    ?.Select(x => x.Substring(x.IndexOf(CategorySeparator, StringComparison.Ordinal) + CategorySeparator.Length))
                                    ?.ToList()
                                ?? new List<string>();
                        })
                    .ConfigureAwait(false);

            //TODO: This could be optimized
            var latestSyncables = await GetAllLatestSyncQueue<T>().ConfigureAwait(false);

            var syncCategories =
                await _processingQueue
                    .Queue(
                        () =>
                        {
                            var syncCollection = GetSynchronizableTypeCollection<T>();

                            return syncCollection
                                    .Find(Query.And(
                                        Query.Not(SynchronizableCategory, null),
                                        Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>())))
                                    .GroupBy(x => x.Category)
                                    .Select(x => x.Key)
                                    .ToList();
                        })
                    .ConfigureAwait(false);

            return syncCategories?.Any() ?? false
                ? typeCollectionNames.Union(syncCategories)
                : typeCollectionNames;
        }

        public async Task<IEnumerable<T>> GetAllLatest<T>(string category = null)
            where T : class
        {
            var allOfType =
                await _processingQueue
                    .Queue(
                        () =>
                        {
                            var typeCollection = GetTypeCollection<T>(category);

                            return typeCollection.FindAll().ToList();
                        })
                    .ConfigureAwait(false);

            var latestSyncables = (await GetAllLatestSyncQueue<T>(category).ConfigureAwait(false)).ToList();

            var rti = _registeredTypes[typeof(T)];

            for (int i = 0; i < latestSyncables.Count; i++)
            {
                var latest = latestSyncables[i];

                var id = rti.GetId(latest);

                var index = allOfType
                    .FindIndex(
                        x =>
                        {
                            var itemId = rti.GetId(x);
                            return itemId.Equals(id, StringComparison.Ordinal);
                        });

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

        public Task<T> GetLatest<T>(T obj, string category = null)
            where T : class
        {
            var rti = _registeredTypes[typeof(T)];

            var id = rti.GetId(obj);

            return GetLatest<T>(id, category);
        }

        public Task<T> GetLatest<T>(string id, string category = null)
            where T : class
        {
            return _processingQueue.Queue(
                () =>
                {
                    var syncCollection = GetSynchronizableTypeCollection<T>();

                    var cacheable =
                        syncCollection
                            .FindOne(
                                Query.And(
                                    Query.All(SynchronizableModifiedTimestampIndex, Query.Descending),
                                    GetItemQueryWithId<T>(id, category)));

                    if (cacheable != null)
                        return cacheable.Value;

                    var typeCollection = GetTypeCollection<T>(category);

                    return typeCollection.FindById(id);
                });
        }

        public Task<IEnumerable<T>> GetAllLatestSyncQueue<T>(string category = null)
            where T: class
        {
            return _processingQueue.Queue(
                () =>
                {
                    var syncCollection = GetSynchronizableTypeCollection<T>();

                    return syncCollection
                        .Find(GetItemQuery<T>(category))
                        ?.OrderByDescending(x => x.ModifiedTimestamp)
                        ?.GroupBy(x => x.TypeId)
                        ?.Where(x => !x.Any(i => i.Operation == (int)ClientOperationType.Delete))
                        ?.Select(x => x.First().Value)
                        ?.ToList()
                        ?? Enumerable.Empty<T>();
                });
        }

        public async Task<bool> PopulateCache<T>(IEnumerable<T> items, string category = null, bool terminateSyncQueueHistory = false)
            where T : class
        {
            if(!(await DropCache<T>(category).ConfigureAwait(false)))
            {
                return false;
            }

            if (terminateSyncQueueHistory && !(await TerminateSyncQueueHisory<T>(category).ConfigureAwait(false)))
                return false;

            return 
                await _processingQueue
                    .Queue(
                        () =>
                        {
                            var typeCollection = GetTypeCollection<T>(category);

                            return typeCollection.InsertBulk(items) == items.Count();
                        })
                    .ConfigureAwait(false);

        }

        public Task<bool> DropCache<T>(string category = null)
        {
            return _processingQueue.Queue(
                () =>
                {
                    var rti = _registeredTypes[typeof(T)];
                    var ctn = rti.GetCategoryTypeName(category);

                    if (!_db.CollectionExists(ctn))
                        return true;

                    return _db.DropCollection(ctn);
                });
        }

        public async Task<bool> DeleteCacheItem<T>(T item, string category = null)
            where T : class
        {
            return
                await _processingQueue
                    .Queue(
                        () =>
                        {
                            var typeCollection = GetTypeCollection<T>(category);

                            return typeCollection.Delete(GetItemQuery<T>(item, category)) == 1;
                        })
                    .ConfigureAwait(false);

        }

        public async Task<bool> UpsertCacheItem<T>(T item, string category = null)
            where T : class
        {
            return
                await _processingQueue
                    .Queue(
                        () =>
                        {
                            var typeCollection = GetTypeCollection<T>(category);
                            var itemQuery = GetItemQuery<T>(item, category);

                            return typeCollection.Upsert(item);
                        })
                    .ConfigureAwait(false);
        }

        public Task<IEnumerable<ClientSyncInfo<T>>> GetSyncHistory<T>(string id, string category = null)
            where T : class
        {
            return _processingQueue.Queue(
                () =>
                {
                    var syncCollection = GetSynchronizableTypeCollection<T>();

                    var cacheables =
                        syncCollection
                            .Find(
                                Query.And(
                                    Query.All(SynchronizableModifiedTimestampIndex, Query.Descending),
                                    GetItemQueryWithId<T>(id, category)))
                            ?.ToList();

                    return
                        cacheables
                            ?.Select(x =>
                                new ClientSyncInfo<T>
                                {
                                    ModifiedOn = x.ModifiedTimestamp,
                                    Operation = (ClientOperationType)x.Operation,
                                    Value = x.Value
                                })
                            ?.ToList()
                        ?? Enumerable.Empty<ClientSyncInfo<T>>();
                });
        }

        public Task<IEnumerable<ClientSyncInfo<T>>> GetSyncHistory<T>(SyncType syncType = SyncType.Latest, string category = null, CategorySearch categorySearch = CategorySearch.FullMatch)
            where T : class
        {
            return _processingQueue.Queue(
                () =>
                {
                    var syncCollection = GetSynchronizableTypeCollection<T>();

                    switch (syncType)
                    {
                        case SyncType.Latest:
                            return syncCollection
                                .Find(GetItemQuery<T>(category, categorySearch))
                                ?.OrderByDescending(x => x.ModifiedTimestamp)
                                ?.GroupBy(x => x.TypeId)
                                ?.Select(
                                    x =>
                                    {
                                        var latest = x.First();

                                        return new ClientSyncInfo<T>
                                        {
                                            ModifiedOn = latest.ModifiedTimestamp,
                                            Operation = (ClientOperationType)latest.Operation,
                                            Value = latest.Value
                                        };
                                    })
                                ?.ToList()
                                ?? Enumerable.Empty<ClientSyncInfo<T>>();
                        case SyncType.FullHistory:
                            return syncCollection
                                .Find(GetItemQuery<T>(category, categorySearch))
                                ?.OrderBy(x => x.ModifiedTimestamp)
                                ?.Select(x => GetAsClientSyncInfo(x))
                                ?.ToList()
                                ?? Enumerable.Empty<ClientSyncInfo<T>>();
                    }

                    return Enumerable.Empty<ClientSyncInfo<T>>();
                });
        }

        public Task<int> GetSyncHistoryCount<T>(SyncType syncType = SyncType.Latest, string category = null, CategorySearch categorySearch = CategorySearch.FullMatch)
            where T : class
        {
            return _processingQueue.Queue(
                () =>
                {
                    var syncCollection = GetSynchronizableTypeCollection<T>();

                    switch (syncType)
                    {
                        case SyncType.Latest:
                            return syncCollection
                                .Count(GetItemQuery<T>(category, categorySearch));
                        case SyncType.FullHistory:
                            return syncCollection
                                .Count(GetItemQuery<T>(category, categorySearch));
                    }

                    return 0;
                });
        }

        private bool ItemExistsAndAvailable<T>(T obj, string category = null)
            where T : class
        {
            var id = GetId(obj);
            return ItemExistsAndAvailableWithId<T>(id, category);
        }

        private bool ItemExistsAndAvailableWithId<T>(string id, string category = null)
            where T : class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            var deleted =
                syncCollection
                    .Count(
                        Query.And(
                            Query.EQ(
                                SynchronizableOperationIndex, (int)ClientOperationType.Delete),
                                GetItemQueryWithId<T>(id, category)));

            if (deleted > 0)
                return false;

            return syncCollection.Count(GetItemQueryWithId<T>(id, category)) > 0;
        }

        private Task<Synchronizable<T>> GetLatestSyncQueue<T>(string id, string category = null)
            where T : class
        {
            return _processingQueue.Queue(
                () =>
                {
                    var syncCollection = GetSynchronizableTypeCollection<T>();

                    var cacheable =
                        syncCollection
                            .FindOne(
                                Query.And(
                                    Query.All(SynchronizableModifiedTimestampIndex, Query.Descending),
                                    GetItemQueryWithId<T>(id, category)));

                    return cacheable;
                });
        }

        private Synchronizable<T> GetAsSynchronizable<T>(T obj, ClientOperationType operationType, string category = null)
            where T : class
        {
            var rti = _registeredTypes[typeof(T)];

            var typeId = rti.GetId(obj);

            if (string.IsNullOrEmpty(typeId))
                throw new Exception("All cachable objects need to have a non-null value for the Id");

            var now = DateTimeOffset.Now;

            return new Synchronizable<T>
            {
                Id = ObjectId.NewObjectId(),
                Category = category,
                TypeId = typeId,
                TypeName = rti.TypeFullName,
                Value = obj,
                ModifiedTimestamp = now.ToUnixTimeMilliseconds(),
                Operation = (int)operationType
            };
        }

        private ClientSyncInfo<T> GetAsClientSyncInfo<T>(Synchronizable<T> synchronizable)
        {
            return new ClientSyncInfo<T>
            {
                ModifiedOn = synchronizable.ModifiedTimestamp,
                Operation = (ClientOperationType)synchronizable.Operation,
                Category = synchronizable.Category,
                Value = synchronizable.Value
            };
        }

        private Query GetItemQuery<T>(T obj, string category = null, CategorySearch categorySearch = CategorySearch.FullMatch)
            where T : class
        {
            var id = GetId<T>(obj);
            return GetItemQueryWithId<T>(id, category, categorySearch);
        }

        private Query GetItemQuery<T>(string category = null, CategorySearch categorySearch = CategorySearch.FullMatch)
            where T : class
        {
            return
                categorySearch == CategorySearch.StartsWith
                    ? Query.And(
                        Query.StartsWith(SynchronizableCategory, category),
                        Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>()))
                    : categorySearch == CategorySearch.Contains
                        ? Query.And(
                            Query.Contains(SynchronizableCategory, category),
                            Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>()))
                        : category != null
                            ? Query.And(
                                Query.EQ(SynchronizableCategory, category),
                                Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>()))
                            : Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>());
        }

        private Query GetItemQueryWithId<T>(string id, string category = null, CategorySearch categorySearch = CategorySearch.FullMatch)
            where T : class
        {
            return
                categorySearch == CategorySearch.StartsWith
                    ? Query.And(
                        Query.EQ(SynchronizableTypeIdIndex, id),
                        Query.StartsWith(SynchronizableCategory, category),
                        Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>()))
                    : categorySearch == CategorySearch.Contains
                        ? Query.And(
                            Query.EQ(SynchronizableTypeIdIndex, id),
                            Query.Contains(SynchronizableCategory, category),
                            Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>()))
                        : category != null
                            ? Query.And(
                                Query.EQ(SynchronizableTypeIdIndex, id),
                                Query.EQ(SynchronizableCategory, category),
                                Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>()))
                            : Query.And(
                                Query.EQ(SynchronizableTypeIdIndex, id),
                                Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>()));
        }

        public async Task Reconcile<T>(IEnumerable<ServerSyncInfo<T>> serverSyncInformation, string category = null)
            where T : class
        {
            foreach (var serverSyncInfo in serverSyncInformation)
            {
                var clientInfo = await GetLatestSyncQueue<T>(serverSyncInfo.Id, category).ConfigureAwait(false);

                var result = _syncReconciler.Reconcile(serverSyncInfo, GetAsClientSyncInfo(clientInfo));

                switch (result)
                {
                    case SyncReconciliationAction.KeepClientValue:
                        await TerminateSyncQueueHisory(clientInfo.Value, category).ConfigureAwait(false);
                        await UpsertCacheItem(clientInfo.Value, category).ConfigureAwait(false);
                        break;
                    case SyncReconciliationAction.RemoveClientValue:
                        await TerminateSyncQueueHisory(clientInfo.Value, category).ConfigureAwait(false);
                        await DeleteCacheItem(clientInfo.Value, category);
                        break;
                    case SyncReconciliationAction.ReplaceWithServerValue:
                        await TerminateSyncQueueHisory(clientInfo.Value, category).ConfigureAwait(false);
                        await UpsertCacheItem(serverSyncInfo.Value, category).ConfigureAwait(false);
                        break;
                    case SyncReconciliationAction.None:
                    default:
                        //This likely occurred because of an error on the server or similar, so we will hold onto the value
                        break;
                }
            }
        }

        private BsonValue GetId<T>(T obj)
            where T : class
        {
            var rti = _registeredTypes[typeof(T)];

            return rti.GetId(obj);
        }

        private string GetTypeFullName<T>()
        {
            var rti = _registeredTypes[typeof(T)];

            return rti.TypeFullName;
        }

        private LiteCollection<Synchronizable<T>> GetSynchronizableTypeCollection<T>()
        {
            return _db.GetCollection<Synchronizable<T>>(SyncCollection);
        }

        private LiteCollection<T> GetTypeCollection<T>(string category = null)
        {
            var rti = _registeredTypes[typeof(T)];
            var ctn = rti.GetCategoryTypeName(category);

            if (!_db.CollectionExists(ctn))
            {
                var collection = _db.GetCollection<T>(ctn);
                collection.EnsureIndex(rti.IdProperty);
                return collection;
            }

            return _db.GetCollection<T>(ctn);
        }
    }
}
