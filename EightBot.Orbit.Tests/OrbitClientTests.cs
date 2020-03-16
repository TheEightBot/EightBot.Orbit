using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using EightBot.Orbit.Client;
using System.Linq;
using Bogus;
using System;
using Bogus.Extensions;
using System.Threading.Tasks;
using FluentAssertions;
using System.ServiceModel.Dispatcher;
using System.Collections;
using System.Collections.Generic;

namespace EightBot.Orbit.Tests
{
    [TestClass]
    public class OrbitClientTests
    {
        OrbitClient _client;

        string _tempDbFile;

        public OrbitClientTests()
        {
            Randomizer.Seed = new Random(42);

            _tempDbFile = Path.GetTempPath();
        }

        [TestInitialize]
        public void Setup()
        {
            _client =
                new OrbitClient()
                    .Initialize(_tempDbFile, additionalConnectionStringParameters: "Mode=Exclusive;", deleteExistingCache: true)
                    .AddTypeRegistration<TestClassA, string>(x => x.StringProperty, requiresIdMapping: true)
                    .AddTypeRegistration<TestClassB, string>(x => x.StringProperty, requiresIdMapping: true)
                    .AddTypeRegistration<TestClassC, int>(x => x.IntProperty, requiresIdMapping: true)
                    .AddTypeRegistration<TestClassD, string, float>(x => x.FloatProperty.ToString(), x => x.FloatProperty, requiresIdMapping: true)
                    .AddTypeRegistration<TestClassE, Guid>(x => x.TestClassId, requiresIdMapping: true)
                    .AddTypeRegistration<string>();
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
        public async Task OrbitClient_ShutdownProcessStartup_ShouldProcess()
        {
            _client.Shutdown();
            _client.Startup();

            var testFile =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 42
                };

            var result = await _client.Create(testFile);

            result.Success
                .Should()
                .BeTrue();

            result.OperationResult
                .Should()
                .Be(ClientOperationType.Create);
        }

        [TestMethod]
        public async Task OrbitClient_Create_ShouldBeSuccessful()
        {
            var testFile = 
                new TestClassA 
                { 
                    StringProperty = "Test Value",
                    IntProperty = 42
                };

            var result = await _client.Create(testFile);

            result.Success
                .Should()
                .BeTrue();

            result.OperationResult
                .Should()
                .Be(ClientOperationType.Create);
        }

        [TestMethod]
        public async Task OrbitClient_CreateWithCategory_ShouldBeSuccessful()
        {
            var testFile =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 42
                };

            var result = await _client.Create(testFile, "category");

            result.Success
                .Should()
                .BeTrue();

            result.OperationResult
                .Should()
                .Be(ClientOperationType.Create);
        }

        [TestMethod]
        public async Task OrbitClient_CreateString_ShouldBeSuccessful()
        {
            var str = "Testing";

            var result = await _client.Create(str);

            result.Success
                .Should()
                .BeTrue();

            result.OperationResult
                .Should()
                .Be(ClientOperationType.Create);
        }

        [TestMethod]
        public async Task OrbitClient_CreateWithObjectThatUsesFuncProperty_ShouldBeSuccessful()
        {
            var testFile =
                new TestClassD
                {
                    FloatProperty = 10.0f,
                    DoubleProperty = 42d
                };

            var result = await _client.Create(testFile);

            result.Success
                .Should()
                .BeTrue();

            result.OperationResult
                .Should()
                .Be(ClientOperationType.Create);
        }

        [TestMethod]
        public async Task OrbitClient_CreateWithObjectThatUsesFuncPropertyWithCategory_ShouldBeSuccessful()
        {
            var testFile =
                new TestClassD
                {
                    FloatProperty = 10.0f,
                    DoubleProperty = 42d
                };

            var result = await _client.Create(testFile, "category");

            result.Success
                .Should()
                .BeTrue();

            result.OperationResult
                .Should()
                .Be(ClientOperationType.Create);
        }

        [TestMethod]
        public async Task OrbitClient_InsertAndGetLatest_ShouldFindMatch()
        {
            var testFile =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 42
                };

            await _client.Create(testFile);
            var found = await _client.GetLatest(testFile);
            Assert.IsTrue(testFile.IntProperty == found.IntProperty);
        }

        [TestMethod]
        public async Task OrbitClient_InsertToCacheAndGetLatest_ShouldFindMatch()
        {
            var testFile =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 42
                };

            await _client.UpsertCacheItem(testFile);
            var found = await _client.GetLatest(testFile);
            Assert.IsTrue(testFile.IntProperty == found.IntProperty);
        }

        [TestMethod]
        public async Task OrbitClient_InsertToCacheWithCategoryAndGetLatest_ShouldFindMatch()
        {
            var category = "category";

            var testFile =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 42
                };

            await _client.UpsertCacheItem(testFile, category);
            var found = await _client.GetLatest(testFile, category);
            Assert.IsTrue(testFile.IntProperty == found.IntProperty);
        }

        [TestMethod]
        public async Task OrbitClient_InsertTestClassEToCacheAndGetLatest_ShouldFindMatch()
        {
            var testFile =
                new TestClassE
                {
                    TestClassId = Guid.NewGuid(),
                    Values = new List<TestClassD> { },
                };

            await _client.UpsertCacheItem(testFile);
            var found = await _client.GetLatest(testFile);
            Assert.IsTrue(testFile.TestClassId == found.TestClassId);
        }

        [TestMethod]
        public async Task OrbitClient_InsertTestClassEToCacheWithCategoryAndGetLatest_ShouldFindMatch()
        {
            var category = "category";

            var testFile =
                new TestClassE
                {
                    TestClassId = Guid.NewGuid(),
                    Values = new List<TestClassD> { },
                };

            await _client.UpsertCacheItem(testFile, category);
            var found = await _client.GetLatest(testFile, category);
            Assert.IsTrue(testFile.TestClassId == found.TestClassId);
        }

        [TestMethod]
        public async Task OrbitClient_InsertStringAndGetLatest_ShouldFindMatch()
        {
            var testStr = "testStr";

            await _client.Create(testStr);
            var found = await _client.GetLatest(testStr);
            Assert.IsTrue(testStr == found);
        }

        [TestMethod]
        public async Task OrbitClient_InsertAndGetLatestWithCategory_ShouldFindMatch()
        {
            var category = "category";

            var testFile =
                new TestClassA
                {
                    StringProperty = "Test Value",
                    IntProperty = 42
                };

            await _client.Create(testFile, category);
            var found = await _client.GetLatest(testFile, category);
            Assert.IsTrue(testFile.IntProperty == found.IntProperty);
        }

        [TestMethod]
        public async Task OrbitClient_InsertWithObjectThatUsesFuncPropertyAndGetLatest_ShouldFindMatch()
        {
            var testFile =
                new TestClassD
                {
                    FloatProperty = 10.0f,
                    DoubleProperty = 42d
                };

            await _client.Create(testFile);
            var found = await _client.GetLatest(testFile);
            Assert.IsTrue(testFile.FloatProperty == found.FloatProperty);
        }

        [TestMethod]
        public async Task OrbitClient_InsertWithObjectThatUsesFuncPropertyAndGetLatestWithCategory_ShouldFindMatch()
        {
            var category = "category";

            var testFile =
                new TestClassD
                {
                    FloatProperty = 10.0f,
                    DoubleProperty = 42d
                };

            await _client.Create(testFile, category);
            var found = await _client.GetLatest(testFile, category);
            Assert.IsTrue(testFile.FloatProperty == found.FloatProperty);
        }

        [TestMethod]
        public async Task OrbitClient_InsertMultipleAndGetAll_CountShouldMatch()
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

            await _client.Create(testFile1);
            await _client.Update(testFile2);
            var found = await _client.GetSyncHistory<TestClassA>(testFile1.StringProperty);
            Assert.IsTrue(found.Count() == expected);
        }

        [TestMethod]
        public async Task OrbitClient_InsertMultipleAndGetAllWithCategory_CountShouldMatch()
        {
            var expected = 2;

            var category = "category";

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

            await _client.Create(testFile1, category);
            await _client.Update(testFile2, category);
            var found = await _client.GetSyncHistory<TestClassA>(testFile1.StringProperty, category);
            Assert.IsTrue(found.Count() == expected);
        }

        [TestMethod]
        public async Task OrbitClient_InsertMultipleAndQuery_DoesGetLatest()
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

            var upsert1 = await _client.Upsert(testFile1);

            upsert1.Success
                .Should()
                .BeTrue();

            upsert1.OperationResult
                .Should()
                .Be(ClientOperationType.Create);

            var upsert2 = await _client.Upsert(testFile2);

            upsert2.Success
                .Should()
                .BeTrue();

            upsert2.OperationResult
                .Should()
                .Be(ClientOperationType.Update);

            var upsert3 = await _client.Upsert(testFile3);

            upsert3.Success
                .Should()
                .BeTrue();

            upsert3.OperationResult
                .Should()
                .Be(ClientOperationType.Update);

            var found = await _client.GetLatest(testFile3);

            Assert.IsTrue(found.IntProperty == testFile3.IntProperty);
        }

        [TestMethod]
        public async Task OrbitClient_InsertMultipleAndQueryWithCategory_DoesGetLatest()
        {
            var category = "category";

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

            await _client.Upsert(testFile1, category);
            await _client.Upsert(testFile2, category);
            await _client.Upsert(testFile3, category);

            var found = await _client.GetLatest(testFile3, category);

            Assert.IsTrue(found.IntProperty == testFile3.IntProperty);
        }

        [TestMethod]
        public async Task OrbitClient_InsertAndDeleteAndInsert_ShouldNotInsert()
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

            var upsert1Result = await _client.Upsert(testFile1);

            upsert1Result.Success
                .Should()
                .BeTrue();

            upsert1Result.OperationResult
                .Should()
                .Be(ClientOperationType.Create);

            var deleteResult = await _client.Delete(testFile2);

            deleteResult.Success
                .Should()
                .BeTrue();

            deleteResult.OperationResult
                .Should()
                .Be(ClientOperationType.Delete);

            var upsert2Result = await _client.Upsert(testFile3);

            upsert2Result.Success
                .Should()
                .BeFalse();

            upsert2Result.OperationResult
                .Should()
                .Be(ClientOperationType.NoOperation);
        }

        [TestMethod]
        public async Task OrbitClient_InsertAndDeleteAndInsertWithCategory_ShouldNotInsert()
        {
            var category = "category";

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

            var upsert1Result = await _client.Upsert(testFile1, category);

            upsert1Result.Success
                .Should()
                .BeTrue();

            upsert1Result.OperationResult
                .Should()
                .Be(ClientOperationType.Create);

            var deleteResult =await _client.Delete(testFile2, category);

            deleteResult.Success
                .Should()
                .BeTrue();

            deleteResult.OperationResult
                .Should()
                .Be(ClientOperationType.Delete);

            var upsert2Result = await _client.Upsert(testFile3, category);

            upsert2Result.Success
                .Should()
                .BeFalse();

            upsert2Result.OperationResult
                .Should()
                .Be(ClientOperationType.NoOperation);
        }

        [TestMethod]
        public async Task OrbitClient_InsertMultipleWithSameKey_ShouldFindRightTypes()
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
                
            await _client.Upsert(testFile1);
            await _client.Upsert(testFile2);

            var foundA = await _client.GetLatest<TestClassA>(id);
            var foundB = await _client.GetLatest<TestClassB>(id);

            Assert.IsTrue(foundA.IntProperty == testFile1.IntProperty);
            Assert.IsTrue(foundB.DoubleProperty == testFile2.DoubleProperty);
        }

        [TestMethod]
        public async Task OrbitClient_InsertMultipleWithSameKeyWithCategory_ShouldFindRightTypes()
        {
            var id = "Test Value";

            var category = "category";

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

            await _client.Upsert(testFile1, category);
            await _client.Upsert(testFile2, category);

            var foundA = await _client.GetLatest<TestClassA>(id, category);
            var foundB = await _client.GetLatest<TestClassB>(id, category);

            Assert.IsTrue(foundA.IntProperty == testFile1.IntProperty);
            Assert.IsTrue(foundB.DoubleProperty == testFile2.DoubleProperty);
        }

        [TestMethod]
        public async Task OrbitClient_GetLatestSyncQueueWithInvalidId_ShouldFindNothing()
        {
            var id = "Test Value";

            var testFile1 =
                new TestClassA
                {
                    StringProperty = id,
                    IntProperty = 42
                };

            await _client.Upsert(testFile1);

            var foundA = await _client.GetAllLatestSyncQueue<TestClassA>(id);

            foundA.Should().BeEmpty();
        }

        [TestMethod]
        public async Task OrbitClient_InsertConcurrent_ShouldNotFail()
        {
            try
            {
                var id1 = "id1";
                var id2 = "id2";

                var max = 100;

                var testFile1 =
                    new TestClassA
                    {
                        StringProperty = id1
                    };

                var testFile2 =
                    new TestClassA
                    {
                        StringProperty = id2
                    };

                var insert1Test =
                    Task.Run(
                        () =>
                        {
                            for (int i = 1; i <= max; i++)
                            {
                                testFile1.IntProperty = i;
                                _client.Upsert(testFile1);
                            }
                        });

                var insert2Test =
                    Task.Run(
                        () =>
                        {
                            for (int i = 1; i <= max; i++)
                            {
                                testFile2.IntProperty = i;
                                _client.Upsert(testFile2);
                            }
                        });

                await Task.WhenAll(insert1Test, insert2Test);

                var found1 = await _client.GetLatest<TestClassA>(id1);
                var found2 = await _client.GetLatest<TestClassA>(id2);

                Assert.IsNotNull(found1);
                Assert.IsTrue(found1.IntProperty == max);

                Assert.IsNotNull(found2);
                Assert.IsTrue(found2.IntProperty == max);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
                Assert.Fail($"Expected to not fail, but received: {ex.Message}");
            }
        }

        [TestMethod]
        public async Task OrbitClient_BulkInsertAndUpdate_ShouldGetNewValue()
        {

            var testObjects = 
                new Faker<TestClassA>()
                    .RuleFor(x => x.StringProperty, (f, u) => $"String_{f.IndexFaker}")
                    .RuleFor(x => x.IntProperty, (f, u) => f.IndexFaker);

            var generatedTestObjects = testObjects.GenerateBetween(100, 100);

            var populated = await _client.PopulateCache(generatedTestObjects);

            Assert.IsTrue(populated);

            var original = generatedTestObjects[49];

            var foundObject = await _client.GetLatest<TestClassA>(original.StringProperty);

            Assert.AreEqual(foundObject.IntProperty, original.IntProperty);

            foundObject.IntProperty = foundObject.IntProperty * 2;

            var upsertResult = await _client.Upsert(foundObject);

            upsertResult.Success
                .Should()
                .BeTrue();

            upsertResult.OperationResult
                .Should()
                .Be(ClientOperationType.Update);

            var latest = await _client.GetAllLatest<TestClassA>();

            var updated = latest.FirstOrDefault(x => x.StringProperty == original.StringProperty);

            Assert.IsTrue(updated.IntProperty == foundObject.IntProperty);
        }

        [TestMethod]
        public async Task OrbitClient_BulkInsertAndUpdateWithCategory_ShouldGetNewValue()
        {

            var testObjects =
                new Faker<TestClassA>()
                    .RuleFor(x => x.StringProperty, (f, u) => $"String_{f.IndexFaker}")
                    .RuleFor(x => x.IntProperty, (f, u) => f.IndexFaker);

            var category = "category";

            var generatedTestObjects = testObjects.GenerateBetween(100, 100);

            var populated = await _client.PopulateCache(generatedTestObjects, category);

            Assert.IsTrue(populated);

            var original = generatedTestObjects[49];

            var foundObject = await _client.GetLatest<TestClassA>(original.StringProperty, category);

            Assert.AreEqual(foundObject.IntProperty, original.IntProperty);

            foundObject.IntProperty = foundObject.IntProperty * 2;

            var upsertResult = await _client.Upsert(foundObject, category);

            upsertResult.Success
                .Should()
                .BeTrue();

            upsertResult.OperationResult
                .Should()
                .Be(ClientOperationType.Update);

            var latest = await _client.GetAllLatest<TestClassA>(category);

            var updated = latest.FirstOrDefault(x => x.StringProperty == original.StringProperty);

            Assert.IsTrue(updated.IntProperty == foundObject.IntProperty);
        }

        [TestMethod]
        public async Task OrbitClient_InsertItemsWithCategories_ShouldGetRightItemForCategory()
        {
            var category1 = "category1";
            var category2 = "category2";

            var testFile1 =
                new TestClassA
                {
                    StringProperty = "test1",
                    IntProperty = 42
                };

            var testFile2 =
                new TestClassA
                {
                    StringProperty = "test2",
                    IntProperty = 84
                };

            await _client.Upsert(testFile1, category1);
            await _client.Upsert(testFile2, category2);

            var testFile1InCategory2 = await _client.GetLatest<TestClassA>(testFile1.StringProperty, category2);
            var testFile2InCategory1 = await _client.GetLatest<TestClassA>(testFile2.StringProperty, category1);

            Assert.IsNull(testFile1InCategory2);
            Assert.IsNull(testFile2InCategory1);

            var testFile1InCategory1 = await _client.GetLatest<TestClassA>(testFile1.StringProperty, category1);
            var testFile2InCategory2 = await _client.GetLatest<TestClassA>(testFile2.StringProperty, category2);

            Assert.IsNotNull(testFile1InCategory1);
            Assert.IsNotNull(testFile2InCategory2);
        }

        [TestMethod]
        public async Task OrbitClient_InsertItemsWithCategories_ShouldFindCategories()
        {
            var category1 = "category1";
            var category2 = "category2";

            var expectedCategories = 2;

            var testFile1 =
                new TestClassA
                {
                    StringProperty = "test1",
                    IntProperty = 42
                };

            var testFile2 =
                new TestClassA
                {
                    StringProperty = "test2",
                    IntProperty = 84
                };

            await _client.Upsert(testFile1, category1);
            await _client.Upsert(testFile2, category2);

            var categories = (await _client.GetCategories<TestClassA>()).ToList();

            Assert.IsTrue(categories.Contains(category1));
            Assert.IsTrue(categories.Contains(category2));
            Assert.IsTrue(categories.Count == expectedCategories);
        }

        [TestMethod]
        public async Task OrbitClient_PopulateCacheAndInsertItemsWithCategories_ShouldFindCategories()
        {
            var category1 = "category1";
            var category2 = "category2";

            var expectedCategories = 2;

            var testFile1 =
                new TestClassA
                {
                    StringProperty = "test1",
                    IntProperty = 42
                };

            var testFile2 =
                new TestClassA
                {
                    StringProperty = "test2",
                    IntProperty = 84
                };

            await _client.PopulateCache(new[] { testFile1 }, category1);
            await _client.Upsert(testFile2, category2);

            var categories = (await _client.GetCategories<TestClassA>()).ToList();

            Assert.IsTrue(categories.Contains(category1));
            Assert.IsTrue(categories.Contains(category2));
            Assert.IsTrue(categories.Count == expectedCategories);
        }

        [TestMethod]
        public async Task OrbitClient_PopulateCacheWithSimpleItems_ShouldPopulate()
        {
            var category1 = "category1";
            var category2 = "category2";

            await _client.PopulateCache(new[] { category1, category2 });

            var latest = await _client.GetAllLatest<string>();

            Assert.IsTrue(latest.Count() == 2);
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

        class TestClassE
        {
            public Guid TestClassId { get; set; }

            public IEnumerable<TestClassD> Values { get; set; }
        }
    }
}
