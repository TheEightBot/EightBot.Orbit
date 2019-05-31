using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using LiteDB;

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

        private readonly Dictionary<Type, RegisteredTypeInformation> _registeredTypes =
            new Dictionary<Type, RegisteredTypeInformation>();

        LiteDatabase _db;

        public static string CategorySeparator { get; set; } = "_-_";

        public string CachePath { get; private set; }

        public bool Initialized { get; private set; }

        public OrbitClient Initialize(string cacheDirectory, string customCacheName = null, string additionalConnectionStringParameters = null)
        {
            if(!Initialized)
            {
                Initialized = true;

                CachePath = Path.Combine(cacheDirectory, customCacheName ?? OrbitCacheDb);

                _db = new LiteDatabase($"Filename={CachePath};{additionalConnectionStringParameters}");

                var syncCollection = _db.GetCollection(SyncCollection);

                syncCollection.EnsureIndex(SynchronizableTypeIdIndex);
                syncCollection.EnsureIndex(SynchronizableCategory);
                syncCollection.EnsureIndex(SynchronizableTypeNameIndex);
                syncCollection.EnsureIndex(SynchronizableModifiedTimestampIndex);
                syncCollection.EnsureIndex(SynchronizableOperationIndex);
            }

            return this;
        }

        public void Shutdown()
        {
            if (!Initialized)
                return;

            try
            {
                _db.Dispose();
            }
            finally
            {
                Initialized = false;
            }
        }

        public OrbitClient AddTypeRegistration<T, TId>(Expression<Func<T, TId>> idSelector, string typeNameOverride = null)
            where T : class
        {
            var rti = RegisteredTypeInformation.Create(idSelector, typeNameOverride);

            _registeredTypes[rti.ObjectType] = rti;

            var typeCollection = _db.GetCollection(rti.TypeName);

            BsonMapper.Global
                .Entity<T>()
                .Id(idSelector, false);

            typeCollection.EnsureIndex(rti.IdProperty);

            return this;
        }

        public OrbitClient AddTypeRegistration<T, TId>(Expression<Func<T, string>> idSelector, Expression<Func<T, TId>> idProperty, string typeNameOverride = null)
            where T : class
        {
            var rti = RegisteredTypeInformation.Create(idSelector, idProperty, typeNameOverride);

            _registeredTypes[rti.ObjectType] = rti;

            var typeCollection = _db.GetCollection(rti.TypeName);

            BsonMapper.Global
                .Entity<T>()
                .Id(idProperty, false);

            typeCollection.EnsureIndex(rti.IdProperty);

            return this;
        }

        public bool Create<T>(T obj, string category = null)
            where T : class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            if (!syncCollection.Exists(GetItemQuery(obj, category)))
            {
                syncCollection.Insert(GetAsSynchronizable(obj, OperationType.Create, category));

                return true;
            }

            return false;
        }

        public bool Update<T>(T obj, string category = null)
            where T : class
        {
            if (ItemExistsAndAvailable(obj, category))
            {
                var syncCollection = GetSynchronizableTypeCollection<T>();
                syncCollection.Insert(GetAsSynchronizable(obj, OperationType.Update, category));
                return true;
            }

            return false;
        }

        public bool Upsert<T>(T obj, string category = null)
            where T : class
        {
            if (Update(obj, category))
            {
                return true;
            }

            return Create(obj, category);
        }

        public bool Delete<T>(T obj, string category = null)
            where T : class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            if (ItemExistsAndAvailable(obj, category))
            {
                syncCollection.Insert(GetAsSynchronizable(obj, OperationType.Delete, category));
                return true;
            }

            return false;
        }

        public bool ReplaceSyncQueueHistory<T>(T obj, string category = null)
            where T : class
        {
            if (!TerminateSyncQueueHisory(obj, category))
                return false;

            return Create(obj, category);
        }

        public bool TerminateSyncQueueHisory<T>(T obj, string category = null)
            where T : class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            return syncCollection.Delete(GetItemQuery(obj, category)) > 0;
        }

        public bool TerminateSyncQueueHistoryAt<T>(T obj, DateTimeOffset offset, string category = null)
            where T : class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            return syncCollection
                .Delete(
                    Query.And(
                        GetItemQuery(obj, category),
                        Query.EQ(SynchronizableModifiedTimestampIndex, offset.ToUnixTimeMilliseconds()))) > 0;
        }

        public IEnumerable<string> GetCategories<T>()
            where T : class
        {
            var rti = _registeredTypes[typeof(T)];
            var ctn = rti.GetCategoryTypeName();

            var typeCollectionNames = 
                _db.GetCollectionNames()
                    ?.Where(x => x.StartsWith(ctn, StringComparison.Ordinal) && x.Contains(CategorySeparator))
                    ?.Select(x => x.Substring(x.IndexOf(CategorySeparator, StringComparison.Ordinal) + CategorySeparator.Length))
                    ?.ToList()
                ?? new List<string>();

            var latestSyncables = GetAllLatestSyncQueue<T>();

            var syncCollection = GetSynchronizableTypeCollection<T>();

            var syncCategories = 
                syncCollection
                    .Find(Query.And(
                        Query.Not(SynchronizableCategory, null),
                        Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>())))
                    .GroupBy(x => x.Category)
                    .Select(x => x.Key)
                    .ToList();

            return syncCategories?.Any() ?? false
                ? typeCollectionNames.Union(syncCategories)
                : typeCollectionNames;
        }

        public IEnumerable<T> GetAllLatest<T>(string category = null)
            where T : class
        {
            var typeCollection = GetTypeCollection<T>(category);

            var allOfType = typeCollection.FindAll().ToList();

            var latestSyncables = GetAllLatestSyncQueue<T>(category).ToList();

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
            }

            return allOfType;
        }

        public T GetLatest<T>(T obj, string category = null)
            where T : class
        {
            var rti = _registeredTypes[typeof(T)];

            var id = rti.GetId(obj);

            return GetLatest<T>(id, category);
        }

        public T GetLatest<T>(string id, string category = null)
            where T : class
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
        }

        public IEnumerable<T> GetAllLatestSyncQueue<T>(string category = null)
            where T: class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            return syncCollection
                .Find(GetItemQuery<T>(category))
                ?.OrderByDescending(x => x.ModifiedTimestamp)
                ?.GroupBy(x => x.TypeId)
                ?.Where(x => !x.Any(i => i.Operation == (int)OperationType.Delete))
                ?.Select(x => x.First().Value)
                ?.ToList()
                ?? Enumerable.Empty<T>();
        }

        public bool PopulateCache<T>(IEnumerable<T> items, string category = null)
        {
            if(!DropTypeCollection<T>(category))
            {
                return false;
            }

            var typeCollection = GetTypeCollection<T>(category);

            return typeCollection.InsertBulk(items) == items.Count();
        }

        public IEnumerable<SyncInfo<T>> GetSyncHistory<T>(string id, string category = null)
            where T : class
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
                        new SyncInfo<T>
                        {
                            ModifiedOn = x.ModifiedTimestamp,
                            Operation = (OperationType)x.Operation,
                            Value = x.Value
                        })
                    ?.ToList()
                ?? Enumerable.Empty<SyncInfo<T>>();
        }

        public IEnumerable<SyncInfo<T>> GetSyncHistory<T>(SyncType syncType = SyncType.Latest, string category = null)
            where T : class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            switch (syncType)
            {
                case SyncType.Latest:
                    return syncCollection
                        .Find(GetItemQuery(category))
                        ?.OrderByDescending(x => x.ModifiedTimestamp)
                        ?.GroupBy(x => x.TypeId)
                        ?.Select(
                            x =>
                            {
                                var latest = x.First();

                                return new SyncInfo<T>
                                {
                                    ModifiedOn = latest.ModifiedTimestamp,
                                    Operation = (OperationType)latest.Operation,
                                    Value = latest.Value
                                };
                            })
                        ?.ToList()
                        ?? Enumerable.Empty<SyncInfo<T>>();
                case SyncType.FullHistory:
                    return syncCollection
                        .Find(GetItemQuery(category))
                        ?.OrderByDescending(x => x.ModifiedTimestamp)
                        ?.Select(
                            x =>
                            {
                                return new SyncInfo<T>
                                {
                                    ModifiedOn = x.ModifiedTimestamp,
                                    Operation = (OperationType)x.Operation,
                                    Value = x.Value
                                };
                            })
                        ?.ToList()
                        ?? Enumerable.Empty<SyncInfo<T>>();
            }

            return Enumerable.Empty<SyncInfo<T>>();
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
                                SynchronizableOperationIndex, (int)OperationType.Delete),
                                GetItemQueryWithId<T>(id, category)));

            if (deleted > 0)
                return false;

            return syncCollection.Count(GetItemQueryWithId<T>(id, category)) > 0;
        }

        private Synchronizable<T> GetAsSynchronizable<T>(T obj, OperationType operationType, string category = null)
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

        private Query GetItemQuery<T>(T obj, string category = null)
            where T : class
        {
            var id = GetId<T>(obj);
            return GetItemQueryWithId<T>(id, category);
        }

        private Query GetItemQuery<T>(string category = null)
            where T : class
        {
            return
                category != null
                ? Query.And(
                    Query.EQ(SynchronizableCategory, category),
                    Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>()))
                : Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>());
        }

        private Query GetItemQueryWithId<T>(string id, string category = null)
            where T : class
        {
            return 
                category != null
                ? Query.And(
                    Query.EQ(SynchronizableTypeIdIndex, id),
                    Query.EQ(SynchronizableCategory, category),
                    Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>()))
                : Query.And(
                    Query.EQ(SynchronizableTypeIdIndex, id),
                    Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>()));
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

        public bool DropTypeCollection<T>(string category = null)
        {
            var rti = _registeredTypes[typeof(T)];
            var ctn = rti.GetCategoryTypeName(category);

            if (!_db.CollectionExists(ctn))
                return true;

            return _db.DropCollection(ctn);
        }
    }
}
