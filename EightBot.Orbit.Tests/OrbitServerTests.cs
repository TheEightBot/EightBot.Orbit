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
        public async Task OrbitServer_OrbitCosmosClient_Insert_One()
        {
            var syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < 1; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"One{i}", IntProperty = 100 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == 1);

            Assert.IsTrue(results.ElementAt(0).StringProperty == "One0");
            Assert.IsTrue(results.ElementAt(0).IntProperty == 100);
        }

        [TestMethod]
        public async Task OrbitServer_OrbitCosmosClient_Insert_OneHundred()
        {
            var syncables = new List<SyncInfo<TestClassA>>();
            for (var i = 0; i < 100; i++)
            {
                syncables.Add(new SyncInfo<TestClassA>()
                {
                    Operation = OperationType.Create,
                    ModifiedOn = DateTimeOffset.Now.ToUnixTimeMilliseconds(),
                    Value = new TestClassA() { StringProperty = $"OneHundred{i}", IntProperty = 100 }
                });
            }

            var results = await this.OrbitDataClient.Sync(syncables);

            Assert.IsTrue(results.Count() == 100);

            Assert.IsTrue(results.ElementAt(0).StringProperty == "OneHundred0");
            Assert.IsTrue(results.ElementAt(0).IntProperty == 100);

            Assert.IsTrue(results.ElementAt(99).StringProperty == "OneHundred99");
            Assert.IsTrue(results.ElementAt(99).IntProperty == 100);
        }

        [TestMethod]
        public void OrbitServer_OrbitCosmosClient_Insert_OneThousand()
        {

        }

        public class TestClassA
        {
            public string StringProperty { get; set; }

            public int IntProperty { get; set; }
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
