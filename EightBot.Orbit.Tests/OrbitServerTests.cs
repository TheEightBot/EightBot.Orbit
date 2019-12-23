using EightBot.Nebula.DocumentDb;
using EightBot.Orbit.Server;
using EightBot.Orbit.Server.Data;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Debug;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EightBot.Orbit.Tests
{
    [TestClass]
    public class OrbitServerTests
    {
        public static readonly LoggerFactory Logger = new LoggerFactory(new[] { new DebugLoggerProvider((x, y) => true) });

        private IOrbitDataClient OrbitDataClient = null;
        private IDataClient DataClient = null;

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

            var comosClient = new CosmosClient(databaseUri, authKey);

            var database = await comosClient.CreateDatabaseIfNotExistsAsync(databaseId, 400);

            var dataClient = new DataClient(database, () => Thread.CurrentPrincipal?.Identity?.Name ?? "test")
            {
                ThrowErrors = true,
                LogError = y => documentDbLogger.LogError(y),
                LogInformation = y => documentDbLogger.LogInformation(y)
            };

            await dataClient.EnsureContainerAsync<TestClassA>(x => x.StringProperty, x => x.IntProperty);

            this.OrbitDataClient = new OrbitCosmosDataClient(dataClient);
            this.DataClient = dataClient;
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
            var syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"One{i}", IntProperty = 100, DoubleProperty = 1.00 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Created);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "One0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 1.00);

            // UPDATE
            syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Update,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"One{i}", IntProperty = 100, DoubleProperty = 1.01 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Updated);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "One0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 1.01);

            var total = await this.DataClient.Document<TestClassA>().WhereAsync(x => x.IntProperty == 100);

            Assert.IsTrue(total.Count == count);

            // DELETE
            syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Delete,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"One{i}", IntProperty = 100, DoubleProperty = 1.02 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Deleted);
            Assert.IsTrue(!String.IsNullOrWhiteSpace(results.ElementAt(0).Id));
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Create_Update_Delete_OneHundred()
        {
            var count = 100;

            // CREATE
            var syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"OneHundred{i}", IntProperty = 100, DoubleProperty = 1.00 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Created);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "OneHundred0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 1.00);

            Assert.IsTrue(results.ElementAt(99).Operation == ServerOperationType.Created);
            Assert.IsTrue(results.ElementAt(99).Value.StringProperty == "OneHundred99");
            Assert.IsTrue(results.ElementAt(99).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(99).Value.DoubleProperty == 1.00);

            // UPDATE
            syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"OneHundred{i}", IntProperty = 100, DoubleProperty = 1.01 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Updated);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "OneHundred0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 1.01);

            Assert.IsTrue(results.ElementAt(99).Operation == ServerOperationType.Updated);
            Assert.IsTrue(results.ElementAt(99).Value.StringProperty == "OneHundred99");
            Assert.IsTrue(results.ElementAt(99).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(99).Value.DoubleProperty == 1.01);

            var total = await this.DataClient.Document<TestClassA>().WhereAsync(x => x.IntProperty == 100);

            Assert.IsTrue(total.Count == count);

            // DELETE
            syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Delete,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"OneHundred{i}", IntProperty = 100, DoubleProperty = 1.02 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Deleted);
            Assert.IsTrue(results.ElementAt(0).Value == null);

            Assert.IsTrue(results.ElementAt(99).Operation == ServerOperationType.Deleted);
            Assert.IsTrue(results.ElementAt(99).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Create_Update_Delete_OneThousand()
        {
            var count = 1000;

            // CREATE
            var syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"OneThousand{i}", IntProperty = 100, DoubleProperty = 1.00 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Created);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "OneThousand0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 1.00);

            Assert.IsTrue(results.ElementAt(999).Operation == ServerOperationType.Created);
            Assert.IsTrue(results.ElementAt(999).Value.StringProperty == "OneThousand999");
            Assert.IsTrue(results.ElementAt(999).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(999).Value.DoubleProperty == 1.00);

            // UPDATE
            syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"OneThousand{i}", IntProperty = 100, DoubleProperty = 1.01 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Updated);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "OneThousand0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 1.01);

            Assert.IsTrue(results.ElementAt(999).Operation == ServerOperationType.Updated);
            Assert.IsTrue(results.ElementAt(999).Value.StringProperty == "OneThousand999");
            Assert.IsTrue(results.ElementAt(999).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(999).Value.DoubleProperty == 1.01);

            var total = this.DataClient.Document<TestClassA>().Count(x => x.IntProperty == 100);

            Assert.IsTrue(total == count);

            // DELETE
            syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Delete,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"OneThousand{i}", IntProperty = 100, DoubleProperty = 1.02 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Deleted);
            Assert.IsTrue(results.ElementAt(0).Value == null);

            Assert.IsTrue(results.ElementAt(99).Operation == ServerOperationType.Deleted);
            Assert.IsTrue(results.ElementAt(999).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Update_Newer()
        {
            var count = 1;

            // CREATE
            var syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"UpdateNewer{i}", IntProperty = 100, DoubleProperty = 1.00 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Created);

            var lastupDatedTime = syncables[0].ModifiedOn + 1000;

            // UPDATE
            syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Update,
                    ModifiedOn = lastupDatedTime,
                    Value = new TestClassA() { StringProperty = $"UpdateNewer{i}", IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Updated);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "UpdateNewer0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 999.99);

            // DELETE
            syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Delete,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"UpdateNewer{i}", IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Deleted);
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Update_Older()
        {
            var count = 1;

            // CREATE
            var syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"UpdateOlder{i}", IntProperty = 100, DoubleProperty = 1.00 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Created);

            var lastupDatedTime = syncables[0].ModifiedOn - 1000;

            // UPDATE
            syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Update,
                    ModifiedOn = lastupDatedTime,
                    Value = new TestClassA() { StringProperty = $"UpdateOlder{i}", IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Updated);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "UpdateOlder0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 1.00);

            // DELETE
            syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Delete,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"UpdateOlder{i}", IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Deleted);
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Create_Empty_Id()
        {
            var count = 1;

            var syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Update,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = "", IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Created);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty.Length == 36);
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 999.99);

            // DELETE
            results = await this.OrbitDataClient.Sync(new List<ClientSyncInfo<TestClassA>>() { new ClientSyncInfo<TestClassA>() { Operation = ClientOperationType.Delete, Value = results.ElementAt(0).Value } });

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Deleted);
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Create_Guid_Empty_Id()
        {
            var count = 1;

            var syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Update,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = Guid.Empty.ToString(), IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.NotModified);
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Create_Null_Id()
        {
            var count = 1;

            var syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Update,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = null, IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Created);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty.Length == 36);
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 999.99);

            // DELETE
            results = await this.OrbitDataClient.Sync(new List<ClientSyncInfo<TestClassA>>() { new ClientSyncInfo<TestClassA>() { Operation = ClientOperationType.Delete, Value = results.ElementAt(0).Value } });

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Deleted);
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Update_NotFound()
        {
            var count = 1;

            var syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Update,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"UpdateNotFound{i}", IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Created);
            Assert.IsTrue(results.ElementAt(0).Value.StringProperty == "UpdateNotFound0");
            Assert.IsTrue(results.ElementAt(0).Value.IntProperty == 100);
            Assert.IsTrue(results.ElementAt(0).Value.DoubleProperty == 999.99);

            // DELETE
            results = await this.OrbitDataClient.Sync(new List<ClientSyncInfo<TestClassA>>() { new ClientSyncInfo<TestClassA>() { Operation = ClientOperationType.Delete, Value = results.ElementAt(0).Value } });

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Deleted);
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Delete_NotFound()
        {
            var count = 1;

            var syncables = new List<ClientSyncInfo<TestClassA>>();
            for (var i = 0; i < count; i++)
            {
                syncables.Add(new ClientSyncInfo<TestClassA>()
                {
                    Operation = ClientOperationType.Delete,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"DeleteNotFound{i}", IntProperty = 100, DoubleProperty = 999.99 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.NotModified);
            Assert.IsTrue(results.ElementAt(0).Value == null);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Its_Complicated()
        {
            var count = 10;

            // CREATE
            var syncables = new List<ClientSyncInfo<TestClassA>>();
            var item1 = new ClientSyncInfo<TestClassA>() { Operation = ClientOperationType.Create, ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(), Value = new TestClassA() { StringProperty = $"UpdateNewer0", IntProperty = 100, DoubleProperty = 1.00 } };
            var item2 = new ClientSyncInfo<TestClassA>() { Operation = ClientOperationType.Create, ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(), Value = new TestClassA() { StringProperty = $"UpdateNewer1", IntProperty = 100, DoubleProperty = 1.00 } };
            var item3 = new ClientSyncInfo<TestClassA>() { Operation = ClientOperationType.Create, ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(), Value = new TestClassA() { StringProperty = $"UpdateNewer2", IntProperty = 100, DoubleProperty = 1.00 } };
            var item4 = new ClientSyncInfo<TestClassA>() { Operation = ClientOperationType.Create, ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(), Value = new TestClassA() { StringProperty = $"UpdateNewer3", IntProperty = 100, DoubleProperty = 1.00 } };
            var item5 = new ClientSyncInfo<TestClassA>() { Operation = ClientOperationType.Create, ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(), Value = new TestClassA() { StringProperty = $"UpdateNewer4", IntProperty = 100, DoubleProperty = 1.00 } };
            var item6 = new ClientSyncInfo<TestClassA>() { Operation = ClientOperationType.Create, ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(), Value = new TestClassA() { StringProperty = $"UpdateNewer5", IntProperty = 100, DoubleProperty = 1.00 } };
            var item7 = new ClientSyncInfo<TestClassA>() { Operation = ClientOperationType.Create, ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(), Value = new TestClassA() { StringProperty = $"UpdateNewer6", IntProperty = 100, DoubleProperty = 1.00 } };
            var item8 = new ClientSyncInfo<TestClassA>() { Operation = ClientOperationType.Create, ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(), Value = new TestClassA() { StringProperty = $"UpdateNewer7", IntProperty = 100, DoubleProperty = 1.00 } };
            var item9 = new ClientSyncInfo<TestClassA>() { Operation = ClientOperationType.Create, ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(), Value = new TestClassA() { StringProperty = $"UpdateNewer8", IntProperty = 100, DoubleProperty = 1.00 } };
            var item10 = new ClientSyncInfo<TestClassA>() { Operation = ClientOperationType.Create, ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(), Value = new TestClassA() { StringProperty = $"UpdateNewer9", IntProperty = 100, DoubleProperty = 1.00 } };

            syncables.Add(item1);
            syncables.Add(item2);
            syncables.Add(item3);
            syncables.Add(item4);
            syncables.Add(item5);
            syncables.Add(item6);
            syncables.Add(item7);
            syncables.Add(item8);
            syncables.Add(item9);
            syncables.Add(item10);

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);
            Assert.IsTrue(results.All(x => x.Operation == ServerOperationType.Created));

            // UPDATE
            item1.ModifiedOn = item1.ModifiedOn - 1000;
            item1.Value.GuidProperty = Guid.Empty;
            item2.ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            item2.Value.GuidProperty = Guid.Empty;
            item3.ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            item3.Value.GuidProperty = Guid.Empty;
            item4.ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            item4.Value.GuidProperty = Guid.Empty;
            item5.ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            item5.Value.GuidProperty = Guid.Empty;
            item6.ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            item6.Value.GuidProperty = Guid.Empty;
            item7.ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            item7.Value.GuidProperty = Guid.Empty;
            item8.ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            item8.Value.GuidProperty = Guid.Empty;
            item9.Operation = ClientOperationType.Delete;
            item10.ModifiedOn = item10.ModifiedOn - 1000;
            item8.Value.GuidProperty = Guid.Empty;

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);
            Assert.IsTrue(results.ElementAt(0).Operation == ServerOperationType.Updated);
            Assert.IsTrue(results.ElementAt(0).Value.GuidProperty != Guid.Empty);
            Assert.IsTrue(results.Skip(1).Take(7).All(x => x.Operation == ServerOperationType.Updated));
            Assert.IsTrue(results.Skip(1).Take(7).All(x => x.Value.GuidProperty == Guid.Empty));
            Assert.IsTrue(results.ElementAt(8).Operation == ServerOperationType.Deleted);
            Assert.IsTrue(results.ElementAt(8).Value == null);
            Assert.IsTrue(results.ElementAt(9).Operation == ServerOperationType.Updated);
            Assert.IsTrue(results.ElementAt(9).Value.GuidProperty != Guid.Empty);


            item1.Operation = ClientOperationType.Delete;
            item2.Operation = ClientOperationType.Delete;
            item3.Operation = ClientOperationType.Delete;
            item4.Operation = ClientOperationType.Delete;
            item5.Operation = ClientOperationType.Delete;
            item6.Operation = ClientOperationType.Delete;
            item7.Operation = ClientOperationType.Delete;
            item8.Operation = ClientOperationType.Delete;
            item9.Operation = ClientOperationType.Delete;
            item10.Operation = ClientOperationType.Delete;

            results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == count);

            Assert.IsTrue(results.Skip(0).Take(8).All(x => x.Operation == ServerOperationType.Deleted));
            Assert.IsTrue(results.ElementAt(8).Operation == ServerOperationType.NotModified);
            Assert.IsTrue(results.ElementAt(9).Operation == ServerOperationType.Deleted);
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
