using System;
using LiteDB;
using System.IO;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Linq;
using DiffMatchPatch;

namespace EightBot.Orbit.Client
{
    public class OrbitClient
    {
        private const string
            OrbitCacheDb = "OrbitCache.db",
            CacheCollection = "Cacheable",

            TypeIdIndex = nameof(Synchronizable<object>.TypeId),
            TypeNameIndex = nameof(Synchronizable<object>.TypeName),
            ModifiedTimestampIndex = nameof(Synchronizable<object>.ModifiedTimestamp),
            OperationIndex = nameof(Synchronizable<object>.Operation);

        private readonly Dictionary<Type, RegisteredTypeInformation> _registeredTypes = 
            new Dictionary<Type, RegisteredTypeInformation>();

        LiteDatabase _db;

        public string CachePath { get; private set; }

        public bool Initialized { get; private set; }

        public OrbitClient Initialize(string cacheDirectory, string customCacheName = null, string additionalConnectionStringParameters = null)
        {
            CachePath = Path.Combine(cacheDirectory, customCacheName ?? OrbitCacheDb);

            _db = new LiteDatabase($"Filename={CachePath};{additionalConnectionStringParameters}");

            var cacheCollection = _db.GetCollection(CacheCollection);

            cacheCollection.EnsureIndex(TypeIdIndex);
            cacheCollection.EnsureIndex(TypeNameIndex);
            cacheCollection.EnsureIndex(ModifiedTimestampIndex);
            cacheCollection.EnsureIndex(OperationIndex);

            Initialized = true;

            return this;
        }

        public void Shutdown()
        {
            if (!Initialized)
                return;

            _db.Dispose();
        }

        public OrbitClient AddTypeRegistration<T>(Expression<Func<T, string>> idSelector)
            where T : class
        {
            var rti = RegisteredTypeInformation.Create(idSelector);

            _registeredTypes[rti.ObjectType] = rti;

            return this;
        }

        public bool Create<T>(T obj)
            where T : class
        {
            var cacheCollection = _db.GetCollection<Synchronizable<T>>(CacheCollection);

            if (!cacheCollection.Exists(GetItemQuery(obj)))
            {
                cacheCollection.Insert(GetAsSynchronizable(obj, OperationType.Create));

                return true;
            }

            return false;
        }

        public bool Update<T>(T obj)
            where T : class
        {
            if (ItemExistsAndAvailable(obj))
            {
                var cacheCollection = _db.GetCollection<Synchronizable<T>>(CacheCollection);
                cacheCollection.Insert(GetAsSynchronizable(obj, OperationType.Update));
                return true;
            }

            return false;
        }

        public bool Upsert<T>(T obj)
            where T : class
        {
            if (Update(obj))
                return true;

            return Create(obj);
        }

        public void ReplaceAll<T>(T obj)
            where T : class
        {
            TerminateAll(obj);
            Create(obj);
        }

        public bool Delete<T>(T obj)
            where T : class
        {
            var cacheCollection = _db.GetCollection<Synchronizable<T>>(CacheCollection);

            if (ItemExistsAndAvailable(obj))
            {
                cacheCollection.Insert(GetAsSynchronizable(obj, OperationType.Delete));
                return true;
            }

            return false;
        }

        public bool TerminateAll<T>(T obj)
            where T : class
        {
            var cacheCollection = _db.GetCollection<Synchronizable<T>>(CacheCollection);

            return cacheCollection.Delete(GetItemQuery(obj)) > 0;
        }

        public bool TerminateAt<T>(T obj, DateTimeOffset offset)
            where T : class
        {
            var cacheCollection = _db.GetCollection<Synchronizable<T>>(CacheCollection);

            return cacheCollection
                .Delete(
                    Query.And(
                        GetItemQuery(obj),
                        Query.EQ(ModifiedTimestampIndex, offset.ToUnixTimeMilliseconds()))) > 0;
        }

        public T GetLatest<T>(string id)
            where T : class
        {
            var cacheCollection = _db.GetCollection<Synchronizable<T>>(CacheCollection);

            if (!ItemExistsAndAvailableWithId<T>(id))
                return default(T);

            var cacheables = 
                cacheCollection
                    .Find(
                        Query.And(
                            Query.All(ModifiedTimestampIndex, Query.Descending),
                            GetItemQueryWithId<T>(id)),
                        limit: 1);

            return cacheables?.FirstOrDefault()?.Value ?? default(T);
        }

        public IEnumerable<(DateTimeOffset ModifiedOn, OperationType Operation, T Value)> GetAll<T>(string id)
            where T : class
        {
            var cacheCollection = _db.GetCollection<Synchronizable<T>>(CacheCollection);

            var cacheables =
                cacheCollection
                    .Find(
                        Query.And(
                            Query.All(ModifiedTimestampIndex, Query.Descending),
                            GetItemQueryWithId<T>(id)));

            return 
                cacheables
                    ?.Select(x => (DateTimeOffset.FromUnixTimeMilliseconds(x.ModifiedTimestamp), (OperationType)x.Operation, x.Value))
                    ?.ToList()
                ?? Enumerable.Empty<(DateTimeOffset ModifiedOn, OperationType Operation, T Value)>();
        }

        public T GetLatestCached<T>(T obj)
            where T : class
        {
            var cacheCollection = _db.GetCollection<Synchronizable<T>>(CacheCollection);

            var cacheable = cacheCollection.FindById(GetId(obj));

            return cacheable?.Value ?? default(T);
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
            var cacheCollection = _db.GetCollection<Synchronizable<T>>(CacheCollection);

            var deleted = cacheCollection.Count(Query.And(Query.EQ(OperationIndex, (int)OperationType.Delete), GetItemQueryWithId<T>(id)));

            if (deleted > 0)
                return false;

            return cacheCollection.Count(GetItemQueryWithId<T>(id)) > 0;
        }

        private Synchronizable<T> GetAsSynchronizable<T>(T obj, OperationType operationType)
            where T : class
        {
            var rti = _registeredTypes[typeof(T)];

            var typeId = rti.IdSelector.GetValue(obj).ToString();

            if (string.IsNullOrEmpty(typeId))
                throw new Exception("All cachable objects need to have a non-null value for the Id");

            var now = DateTimeOffset.Now;

            return new Synchronizable<T>
            {
                Id = ObjectId.NewObjectId(),
                TypeId = typeId,
                TypeName = rti.ObjectTypeName,
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
                Query.EQ(TypeIdIndex, id),
                Query.EQ(TypeNameIndex, GetObjectTypeName<T>()));
        }

        private BsonValue GetId<T>(T obj)
            where T : class
        {
            var rti = _registeredTypes[typeof(T)];

            return rti.IdSelector.GetValue(obj).ToString();
        }

        private string GetObjectTypeName<T>()
        {
            var rti = _registeredTypes[typeof(T)];

            return rti.ObjectTypeName;
        }
    }

    internal class RegisteredTypeInformation
    {
        public PropertyInfo IdSelector { get; set; }

        public string ObjectTypeName { get; set; }

        public Type ObjectType { get; set; }

        public static RegisteredTypeInformation Create<T>(Expression<Func<T, string>> idSelector)
        {
            if(idSelector.Body is MemberExpression mex && mex.Member is PropertyInfo pi)
            {
                var rti =
                    new RegisteredTypeInformation
                    {
                        IdSelector = pi,
                        ObjectTypeName = typeof(T).FullName,
                        ObjectType = typeof(T)
                    };

                return rti;
            }

            throw new ArgumentException($"The expression provided is not a property selector for {typeof(T).Name}", nameof(idSelector));
        }
    }
}
