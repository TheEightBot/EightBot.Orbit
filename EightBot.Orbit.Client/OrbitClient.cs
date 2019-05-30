using System;
using LiteDB;
using System.IO;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Linq;
using DiffMatchPatch;
using System.Security.Cryptography;

namespace EightBot.Orbit.Client
{
    public class OrbitClient
    {
        private const string
            OrbitCacheDb = "OrbitCache.db",
            SyncCollection = "Synchronizable",

            SynchronizableTypeIdIndex = nameof(Synchronizable<object>.TypeId),
            SynchronizableTypeNameIndex = nameof(Synchronizable<object>.TypeName),
            SynchronizableModifiedTimestampIndex = nameof(Synchronizable<object>.ModifiedTimestamp),
            SynchronizableOperationIndex = nameof(Synchronizable<object>.Operation);

        private readonly Dictionary<Type, RegisteredTypeInformation> _registeredTypes = 
            new Dictionary<Type, RegisteredTypeInformation>();

        LiteDatabase _db;

        public string CachePath { get; private set; }

        public bool Initialized { get; private set; }

        public OrbitClient Initialize(string cacheDirectory, string customCacheName = null, string additionalConnectionStringParameters = null)
        {
            CachePath = Path.Combine(cacheDirectory, customCacheName ?? OrbitCacheDb);

            _db = new LiteDatabase($"Filename={CachePath};{additionalConnectionStringParameters}");

            var syncCollection = _db.GetCollection(SyncCollection);

            syncCollection.EnsureIndex(SynchronizableTypeIdIndex);
            syncCollection.EnsureIndex(SynchronizableTypeNameIndex);
            syncCollection.EnsureIndex(SynchronizableModifiedTimestampIndex);
            syncCollection.EnsureIndex(SynchronizableOperationIndex);

            Initialized = true;

            return this;
        }

        public void Shutdown()
        {
            if (!Initialized)
                return;

            _db.Dispose();
        }

        public OrbitClient AddTypeRegistration<T, TId>(Expression<Func<T, TId>> idSelector)
            where T : class
        {
            var rti = RegisteredTypeInformation.Create(idSelector);

            _registeredTypes[rti.ObjectType] = rti;

            var typeCollection = _db.GetCollection(rti.TypeName);

            BsonMapper.Global
                .Entity<T>()
                .Id(idSelector, false);

            typeCollection.EnsureIndex(rti.IdProperty);

            return this;
        }

        public bool Create<T>(T obj)
            where T : class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            if (!syncCollection.Exists(GetItemQuery(obj)))
            {
                syncCollection.Insert(GetAsSynchronizable(obj, OperationType.Create));

                return true;
            }

            return false;
        }

        public bool Update<T>(T obj)
            where T : class
        {
            if (ItemExistsAndAvailable(obj))
            {
                var syncCollection = GetSynchronizableTypeCollection<T>();
                syncCollection.Insert(GetAsSynchronizable(obj, OperationType.Update));
                return true;
            }

            return false;
        }

        public bool Upsert<T>(T obj)
            where T : class
        {
            if (Update(obj))
            {
                return true;
            }

            return Create(obj);
        }

        public bool Delete<T>(T obj)
            where T : class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            if (ItemExistsAndAvailable(obj))
            {
                syncCollection.Insert(GetAsSynchronizable(obj, OperationType.Delete));
                return true;
            }
            
            return false;
        }

        public bool ReplaceSyncQueueHistory<T>(T obj)
            where T : class
        {
            if (!TerminateSyncQueueHisory(obj))
                return false;

            return Create(obj);
        }

        public bool TerminateSyncQueueHisory<T>(T obj)
            where T : class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            return syncCollection.Delete(GetItemQuery(obj)) > 0;
        }

        public bool TerminateSyncQueueHistoryAt<T>(T obj, DateTimeOffset offset)
            where T : class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            return syncCollection
                .Delete(
                    Query.And(
                        GetItemQuery(obj),
                        Query.EQ(SynchronizableModifiedTimestampIndex, offset.ToUnixTimeMilliseconds()))) > 0;
        }

        public IEnumerable<T> GetLatest<T>()
            where T : class
        {
            var typeCollection = GetTypeCollection<T>();

            var allOfType = typeCollection.FindAll().ToList();

            var latestSyncables = GetLatestSyncQueue<T>();

            var rti = _registeredTypes[typeof(T)];

            foreach (var latest in latestSyncables)
            {
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

        public T GetLatest<T>(string id)
            where T : class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            var cacheable = 
                syncCollection
                    .FindOne(
                        Query.And(
                            Query.All(SynchronizableModifiedTimestampIndex, Query.Descending),
                            GetItemQueryWithId<T>(id)));

            if (cacheable != null)
                return cacheable.Value;

            var typeCollection = GetTypeCollection<T>();

            return typeCollection.FindById(id);
        }

        public IEnumerable<T> GetLatestSyncQueue<T>()
            where T: class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            return syncCollection
                .Find(Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>()))
                ?.OrderByDescending(x => x.ModifiedTimestamp)
                ?.GroupBy(x => x.TypeId)
                ?.Where(x => !x.Any(i => i.Operation == (int)OperationType.Delete))
                ?.Select(x => x.First().Value)
                ?.ToList()
                ?? Enumerable.Empty<T>();
        }

        public bool PopulateCache<T>(IEnumerable<T> items)
        {
            if(!DropCache<T>())
            {
                return false;
            }

            var typeCollection = GetTypeCollection<T>();

            return typeCollection.InsertBulk(items) == items.Count();
        }

        public bool DropCache<T>()
        {
            var rti = _registeredTypes[typeof(T)];

            return _db.DropCollection(rti.TypeName);
        }

        public IEnumerable<SyncInfo<T>> GetSyncHistory<T>(string id)
            where T : class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            var cacheables =
                syncCollection
                    .Find(
                        Query.And(
                            Query.All(SynchronizableModifiedTimestampIndex, Query.Descending),
                            GetItemQueryWithId<T>(id)))
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

        public IEnumerable<SyncInfo<T>> GetSyncHistory<T>(SyncType syncType = SyncType.Latest)
            where T : class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            switch (syncType)
            {
                case SyncType.Latest:
                    return syncCollection
                        .Find(Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>()))
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
                        .Find(Query.EQ(SynchronizableTypeNameIndex, GetTypeFullName<T>()))
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

        private bool ItemExistsAndAvailable<T>(T obj)
            where T : class
        {
            var id = GetId(obj);
            return ItemExistsAndAvailableWithId<T>(id);
        }

        private bool ItemExistsAndAvailableWithId<T>(string id)
            where T : class
        {
            var syncCollection = GetSynchronizableTypeCollection<T>();

            var deleted = 
                syncCollection
                    .Count(
                        Query.And(
                            Query.EQ(
                                SynchronizableOperationIndex, (int)OperationType.Delete), 
                                GetItemQueryWithId<T>(id)));

            if (deleted > 0)
                return false;

            return syncCollection.Count(GetItemQueryWithId<T>(id)) > 0;
        }

        private Synchronizable<T> GetAsSynchronizable<T>(T obj, OperationType operationType)
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
                TypeId = typeId,
                TypeName = rti.TypeFullName,
                Value = obj,
                ModifiedTimestamp = now.ToUnixTimeMilliseconds(),
                Operation = (int)operationType
            };
        }

        private Query GetItemQuery<T>(T obj)
            where T : class
        {
            var id = GetId<T>(obj);
            return GetItemQueryWithId<T>(id);
        }

        private Query GetItemQueryWithId<T>(string id)
            where T : class
        {
            return Query.And(
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

        private string GetTypeName<T>()
        {
            var rti = _registeredTypes[typeof(T)];

            return rti.TypeName;
        }

        private LiteCollection<Synchronizable<T>> GetSynchronizableTypeCollection<T>()
        {
            return _db.GetCollection<Synchronizable<T>>(SyncCollection);
        }

        private LiteCollection<T> GetTypeCollection<T>()
        {
            var rti = _registeredTypes[typeof(T)];

            var typeCollection = _db.GetCollection<T>(rti.TypeName);

            typeCollection.EnsureIndex(rti.IdProperty);

            return typeCollection;
        }
    }

    internal class RegisteredTypeInformation
    {
        public PropertyInfo PropertyIdSelector { get; set; }

        public Delegate FuncIdSelector { get; set; }

        public string IdProperty { get; set; }

        public string TypeFullName { get; set; }

        public string TypeName { get; set; }

        public string TypeNamespace { get; set; }

        public Type ObjectType { get; set; }

        public static RegisteredTypeInformation Create<T, TId>(Expression<Func<T, TId>> idSelector, string typeNameOverride = null)
        {
            if(idSelector.Body is MemberExpression mex && mex.Member is PropertyInfo pi)
            {
                var type = typeof(T);

                var rti =
                    new RegisteredTypeInformation
                    {
                        PropertyIdSelector = pi,
                        IdProperty = pi.Name,
                        TypeFullName = type.FullName,
                        TypeName = typeNameOverride ?? type.Name,
                        TypeNamespace = type.Namespace,
                        ObjectType = type
                    };

                return rti;
            }

            throw new ArgumentException($"The expression provided is not a property selector for {typeof(T).Name}", nameof(idSelector));
        }

        public static RegisteredTypeInformation Create<T, TId>(Expression<Func<T, string>> idSelector, Expression<Func<T, TId>> idProperty, string typeNameOverride = null)
        {
            if(idSelector is LambdaExpression lex && idProperty.Body is MemberExpression mex && mex.Member is PropertyInfo pi)
            {
                var compiledExpression = lex.Compile();
                var type = typeof(T);

                var rti =
                    new RegisteredTypeInformation
                    {
                        FuncIdSelector = compiledExpression,
                        IdProperty = pi.Name,
                        TypeFullName = type.FullName,
                        TypeName = typeNameOverride ?? type.Name,
                        TypeNamespace = type.Namespace,
                        ObjectType = type
                    };

                return rti;
            }

            throw new ArgumentException($"The expression provided is not a lambda expression for {typeof(T).Name}", nameof(idSelector));
        }

        public string GetId<T>(T value)
        {
            if (PropertyIdSelector != null)
                return PropertyIdSelector.GetValue(value).ToString();

            return ((Func<T, string>)FuncIdSelector)(value);
        }
    }
}
