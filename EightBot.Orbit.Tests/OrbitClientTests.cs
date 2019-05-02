using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using EightBot.Orbit.Client;
using System.Linq;

namespace EightBot.Orbit.Tests
{
    [TestClass]
    public class OrbitClientTests
    {
        OrbitClient _client;

        [TestInitialize]
        public void Setup()
        {
            var tempPath = Path.GetTempPath();

            _client = 
                new OrbitClient()
                    .AddTypeRegistration<TestClassA>(x => x.StringProperty)
                    .AddTypeRegistration<TestClassB>(x => x.StringProperty)
                    .Initialize(tempPath, additionalConnectionStringParameters: "Mode=Exclusive;");
        }

        [TestCleanup]
        public void Shutdown()
        {
            _client.Shutdown();

            File.Delete(_client.CachePath);
        }

        [TestMethod]
        public void OrbitClient_Initialize_InitializesSuccessfully()
        {
            Assert.IsTrue(_client.Initialized);
        }

        [TestMethod]
        public void OrbitClient_Create_ShouldBeSuccessful()
        {
            var testFile = 
                new TestClassA 
                { 
                    StringProperty = "Test Value",
                    IntProperty = 42
                };

            var result = _client.Create(testFile);
            Assert.IsTrue(result);
        }

        [TestMethod]
        public void OrbitClient_InsertAndGetLatest_ShouldFindMatch()
        {
            var testFile =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 42
                };

            _client.Create(testFile);
            var found = _client.GetLatest<TestClassA>(testFile.StringProperty);
            Assert.IsTrue(testFile.IntProperty == found.IntProperty);
        }

        [TestMethod]
        public void OrbitClient_InsertMultipleAndGetAll_CountShouldMatch()
        {
            var expected = 2;

            var testFile1 =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 42
                };

            var testFile2 =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 84
                };

            _client.Create(testFile1);
            _client.Update(testFile2);
            var found = _client.GetAll<TestClassA>(testFile1.StringProperty);
            Assert.IsTrue(found.Count() == expected);
        }

        [TestMethod]
        public void OrbitClient_InsertMultipleAndQuery_DoesGetLatest()
        {
            var testFile1 =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 42
                };

            var testFile2 =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 84
                };

            var testFile3 =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 168
                };

            _client.Upsert(testFile1);
            _client.Upsert(testFile2);
            _client.Upsert(testFile3);

            var found = _client.GetLatest<TestClassA>(testFile3.StringProperty);

            Assert.IsTrue(found.IntProperty == testFile3.IntProperty);
        }

        [TestMethod]
        public void OrbitClient_InsertAndDeleteAndInsert_ShouldNotInsert()
        {
            var expectedResult = false;

            var testFile1 =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 42
                };

            var testFile2 =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 84
                };

            var testFile3 =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 168
                };

            _client.Upsert(testFile1);
            _client.Delete(testFile2);
            var upsertResult = _client.Upsert(testFile3);

            Assert.IsTrue(expectedResult == upsertResult);
        }

        [TestMethod]
        public void OrbitClient_InsertMultipleWithSameKey_ShouldFindRightTypes()
        {
            var id = "Test Value";

            var testFile1 =
                new TestClassA
                {
                    StringProperty = id,
                    IntProperty = 42
                };

            var testFile2 =
                new TestClassB
                {
                    StringProperty = id,
                    DoubleProperty = 42d
                };
                
            _client.Upsert(testFile1);
            _client.Upsert(testFile2);

            var foundA = _client.GetLatest<TestClassA>(id);
            var foundB = _client.GetLatest<TestClassB>(id);

            Assert.IsTrue(foundA.IntProperty == testFile1.IntProperty);
            Assert.IsTrue(foundB.DoubleProperty == testFile2.DoubleProperty);
        }

        class TestClassA
        {
            public string StringProperty { get; set; }

            public int IntProperty { get; set; }
        }


        class TestClassB
        {
            public string StringProperty { get; set; }

            public double DoubleProperty { get; set; }
        }
    }
}
