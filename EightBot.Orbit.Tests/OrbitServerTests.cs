using EightBot.Nebula.DocumentDb;
using EightBot.Orbit.Server;
using EightBot.Orbit.Server.Data;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Debug;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace EightBot.Orbit.Tests
{
    [TestClass]
    public class OrbitServerTests
    {
        public static readonly LoggerFactory Logger = new LoggerFactory(new[] { new DebugLoggerProvider((x, y) => true) });

        private IOrbitDataClient OrbitDataClient = null;

        public OrbitServerTests()
        {

        }

        [TestInitialize]
        public async Task Setup()
        {
            var databaseUri = "https://localhost:8081";
            var authKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
            var databaseId = "EightBot.Orbit.Test";

            var documentDbLogger = Logger.CreateLogger("EightBot.Nebula.DocumentDb");

            var dataClient = new DataClient(databaseUri, authKey, databaseId, null)
            {
                ThrowErrors = true,
                LogError = y => documentDbLogger?.LogError(y),
                LogInformation = y => documentDbLogger?.LogInformation(y)
            };

            await dataClient.Client.CreateDatabaseIfNotExistsAsync(new Database { Id = databaseId }, new RequestOptions() { OfferThroughput = 400 });

            await dataClient.EnsureCollectionAsync<TestClassA>(x => x.StringProperty, x => x.IntProperty);

            this.OrbitDataClient = new OrbitCosmosClient(dataClient);
        }

        [TestCleanup]
        public void Shutdown()
        {

        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Create_Update_Delete_One()
        {
            var count = 1;

            // CREATE
            var syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"One{i}", IntProperty = 100, DoubleProperty = 1.00 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Created);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "One0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 1.00);

            // UPDATE
            syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Update,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"One{i}", IntProperty = 100, DoubleProperty = 1.01 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Updated);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "One0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 1.01);

            // DELETE
            syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Delete,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"One{i}", IntProperty = 100, DoubleProperty = 1.02 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Deleted);
            Assert.IsTrue(!String.IsNullOrWhiteSpace(results.ElementAt(0).Id));
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Create_Update_Delete_OneHundred()
        {
            var count = 100;

            // CREATE
            var syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"OneHundred{i}", IntProperty = 100, DoubleProperty = 1.00 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Created);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "OneHundred0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 1.00);

            Assert.IsTrue(results.ElementAt(99).Operation == OperationType.Created);
            Assert.IsTrue(results.ElementAt(99).Value.StringProperty == "OneHundred99");
            Assert.IsTrue(results.ElementAt(99).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(99).Value.DoubleProperty == 1.00);

            // UPDATE
            syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"OneHundred{i}", IntProperty = 100, DoubleProperty = 1.01 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Updated);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "OneHundred0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 1.01);

            Assert.IsTrue(results.ElementAt(99).Operation == OperationType.Updated);
            Assert.IsTrue(results.ElementAt(99).Value.StringProperty == "OneHundred99");
            Assert.IsTrue(results.ElementAt(99).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(99).Value.DoubleProperty == 1.01);

            // DELETE
            syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Delete,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"OneHundred{i}", IntProperty = 100, DoubleProperty = 1.02 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Deleted);
            Assert.IsTrue(results.ElementAt(0).Value == null);

            Assert.IsTrue(results.ElementAt(99).Operation == OperationType.Deleted);
            Assert.IsTrue(results.ElementAt(99).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Create_Update_Delete_OneThousand()
        {
            var count = 1000;

            // CREATE
            var syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"OneThousand{i}", IntProperty = 100, DoubleProperty = 1.00 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Created);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "OneThousand0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 1.00);

            Assert.IsTrue(results.ElementAt(999).Operation == OperationType.Created);
            Assert.IsTrue(results.ElementAt(999).Value.StringProperty == "OneThousand999");
            Assert.IsTrue(results.ElementAt(999).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(999).Value.DoubleProperty == 1.00);

            // UPDATE
            syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"OneThousand{i}", IntProperty = 100, DoubleProperty = 1.01 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Updated);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "OneThousand0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 1.01);

            Assert.IsTrue(results.ElementAt(999).Operation == OperationType.Updated);
            Assert.IsTrue(results.ElementAt(999).Value.StringProperty == "OneThousand999");
            Assert.IsTrue(results.ElementAt(999).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(999).Value.DoubleProperty == 1.01);

            // DELETE
            syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Delete,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"OneThousand{i}", IntProperty = 100, DoubleProperty = 1.02 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Deleted);
            Assert.IsTrue(results.ElementAt(0).Value == null);

            Assert.IsTrue(results.ElementAt(99).Operation == OperationType.Deleted);
            Assert.IsTrue(results.ElementAt(999).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Update_Newer()
        {
            var count = 1;

            // CREATE
            var syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"UpdateNewer{i}", IntProperty = 100, DoubleProperty = 1.00 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Created);

            var lastupDatedTime = syncables[0].ModifiedOn + 1000;

            // UPDATE
            syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Update,
                    ModifiedOn = lastupDatedTime,
                    Value = new TestClassA() { StringProperty = $"UpdateNewer{i}", IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Updated);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "UpdateNewer0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 999.99);

            // DELETE
            syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Delete,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"UpdateNewer{i}", IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Deleted);
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Update_Older()
        {
            var count = 1;

            // CREATE
            var syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"UpdateOlder{i}", IntProperty = 100, DoubleProperty = 1.00 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Created);

            var lastupDatedTime = syncables[0].ModifiedOn - 1000;

            // UPDATE
            syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Update,
                    ModifiedOn = lastupDatedTime,
                    Value = new TestClassA() { StringProperty = $"UpdateOlder{i}", IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Updated);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "UpdateOlder0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 1.00);

            // DELETE
            syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Delete,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"UpdateOlder{i}", IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Deleted);
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Create_Empty_Id()
        {
            var count = 1;

            var syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Update,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = "", IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Created);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty.Length == 36);
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 999.99);

            // DELETE
            results = await this.OrbitDataClient.Sync(new List<SyncInfo<TestClassA>>() { new SyncInfo<TestClassA>() { Operation = OperationType.Delete, Value = results.ElementAt(0).Value } });

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Deleted);
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Create_Guid_Empty_Id()
        {
            var count = 1;

            var syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Update,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = Guid.Empty.ToString(), IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.NotModified);
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Create_Null_Id()
        {
            var count = 1;

            var syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Update,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = null, IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Created);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty.Length == 36);
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 999.99);

            // DELETE
            results = await this.OrbitDataClient.Sync(new List<SyncInfo<TestClassA>>() { new SyncInfo<TestClassA>() { Operation = OperationType.Delete, Value = results.ElementAt(0).Value } });

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Deleted);
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Update_NotFound()
        {
            var count = 1;

            var syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Update,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"UpdateNotFound{i}", IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Created);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "UpdateNotFound0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 999.99);

            // DELETE
            results = await this.OrbitDataClient.Sync(new List<SyncInfo<TestClassA>>() { new SyncInfo<TestClassA>() { Operation = OperationType.Delete, Value = results.ElementAt(0).Value } });

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.Deleted);
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Delete_NotFound()
        {
            var count = 1;

            var syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Delete,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"DeleteNotFound{i}", IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == OperationType.NotModified);
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }



        public class TestClassA
        {
            public string StringProperty { get; set; }

            public int IntProperty { get; set; }

            public double DoubleProperty { get; set; }

            public Guid GuidProperty { get; set; } = Guid.NewGuid();

            public DateTime DateTimeProperty { get; set; } = DateTime.Now;
        }


        class TestClassB
        {
            public string StringProperty { get; set; }

            public double DoubleProperty { get; set; }
        }

        class TestClassC
        {
            public int IntProperty { get; set; }

            public string DoubleProperty { get; set; }
        }

        class TestClassD
        {
            public float FloatProperty { get; set; }

            public double DoubleProperty { get; set; }
        }
    }
}
