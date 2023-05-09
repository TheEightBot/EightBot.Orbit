using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Bogus;
using EightBot.Orbit.Client;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tycho;

namespace EightBot.Orbit.Tests
{
    [TestClass]
    [DoNotParallelize]
    public class OrbitClientTests
    {
        OrbitClient _client;

        readonly string _tempDbFile;

        readonly Faker _globalFaker;

        public OrbitClientTests()
        {
            Randomizer.Seed = new Random(42);

            _globalFaker = new Faker();

            _tempDbFile = Path.GetTempPath();
        }

        [TestInitialize]
        public void Setup()
        {
            _client =
                new OrbitClient(new NewtonsoftJsonSerializer())
                    .Initialize(_tempDbFile, $"{Guid.NewGuid()}.db", deleteExistingCache: true)
                    .AddTypeRegistration<TestClassA, string>(x => x.StringProperty)
                    .AddTypeRegistration<TestClassB, string>(x => x.StringProperty)
                    .AddTypeRegistration<TestClassC, int>(x => x.IntProperty)
                    .AddTypeRegistration<TestClassE, Guid>(x => x.TestClassId)
                    .AddTypeRegistrationWithCustomKeySelector<TestClassD>(x => $"{x.FloatProperty}_{x.DoubleProperty}")
                    .AddTypeRegistrationWithCustomKeySelector<string>(x => x)
                    .Startup();
        }

        [TestCleanup]
        public void Shutdown()
        {
            _client.Dispose();

            File.Delete(_client.CachePath);
        }

        [TestMethod]
        [DoNotParallelize]
        public void OrbitClient_Initialize_InitializesSuccessfully()
        {
            Assert.IsTrue(_client.Initialized);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_ShutdownProcessStartup_ShouldProcess()
        {
            _client.Shutdown();
            _client.Startup();

            var stringValue = Guid.NewGuid().ToString();

            var testFile =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = _globalFaker.Random.Int(),
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
        [DoNotParallelize]
        public async Task OrbitClient_Create_ShouldBeSuccessful()
        {
            var stringValue = Guid.NewGuid().ToString();

            var testFile = 
                new TestClassA 
                { 
                    StringProperty = stringValue,
                    IntProperty = _globalFaker.Random.Int(),
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
        [DoNotParallelize]
        public async Task OrbitClient_CreateWithPartition_ShouldBeSuccessful()
        {
            var stringValue = Guid.NewGuid().ToString();

            var testFile =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = _globalFaker.Random.Int(),
                };

            var result = await _client.Create(testFile, "partition");

            result.Success
                .Should()
                .BeTrue();

            result.OperationResult
                .Should()
                .Be(ClientOperationType.Create);
        }

        [TestMethod]
        [DoNotParallelize]
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
        [DoNotParallelize]
        public async Task OrbitClient_CreateWithObjectThatUsesFuncProperty_ShouldBeSuccessful()
        {
            var testFile =
                new TestClassD
                {
                    FloatProperty = 10.0f,
                    DoubleProperty = _globalFaker.Random.Double(),
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
        [DoNotParallelize]
        public async Task OrbitClient_CreateWithObjectThatUsesFuncPropertyWithPartition_ShouldBeSuccessful()
        {
            var testFile =
                new TestClassD
                {
                    FloatProperty = 10.0f,
                    DoubleProperty = _globalFaker.Random.Double(),
                };

            var result = await _client.Create(testFile, "partition");

            result.Success
                .Should()
                .BeTrue();

            result.OperationResult
                .Should()
                .Be(ClientOperationType.Create);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertAndGetLatest_ShouldFindMatch()
        {
            var stringValue = Guid.NewGuid().ToString();
            var testFile =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = _globalFaker.Random.Int(),
                };

            await _client.Create(testFile);
            var found = await _client.GetLatest(testFile);
            Assert.IsTrue(testFile.IntProperty == found.IntProperty);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertToCacheAndGetLatest_ShouldFindMatch()
        {
            var stringValue = Guid.NewGuid().ToString();
            var testFile =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = _globalFaker.Random.Int(),
                };

            await _client.UpsertCacheItem(testFile);
            var found = await _client.GetLatest(testFile);
            Assert.IsTrue(testFile.IntProperty == found.IntProperty);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertToCacheWithPartitionAndGetLatest_ShouldFindMatch()
        {
            var stringValue = Guid.NewGuid().ToString();
            var partition = "partition";

            var testFile =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = _globalFaker.Random.Int(),
                };

            await _client.UpsertCacheItem(testFile, partition);
            var found = await _client.GetLatest(testFile, partition);
            Assert.IsTrue(testFile.IntProperty == found.IntProperty);
        }

        [TestMethod]
        [DoNotParallelize]
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
        [DoNotParallelize]
        public async Task OrbitClient_InsertTestClassEToCacheWithPartitionAndGetLatest_ShouldFindMatch()
        {
            var partition = "partition";

            var testFile =
                new TestClassE
                {
                    TestClassId = Guid.NewGuid(),
                    Values = new List<TestClassD> { },
                };

            await _client.UpsertCacheItem(testFile, partition);
            var found = await _client.GetLatest(testFile, partition);
            Assert.IsTrue(testFile.TestClassId == found.TestClassId);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertStringAndGetLatest_ShouldFindMatch()
        {
            var testStr = "testStr";

            await _client.Create(testStr);
            var found = await _client.GetLatest(testStr);
            Assert.IsTrue(testStr == found);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertAndGetLatestWithPartition_ShouldFindMatch()
        {
            var stringValue = Guid.NewGuid().ToString();
            var partition = "partition";

            var testFile =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = _globalFaker.Random.Int(),
                };

            await _client.Create(testFile, partition);
            var found = await _client.GetLatest(testFile, partition);
            Assert.IsTrue(testFile.IntProperty == found.IntProperty);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertWithObjectThatUsesFuncPropertyAndGetLatest_ShouldFindMatch()
        {
            var testFile =
                new TestClassD
                {
                    FloatProperty = 10.0f,
                    DoubleProperty = _globalFaker.Random.Double(),
                };

            await _client.Create(testFile);
            var found = await _client.GetLatest(testFile);
            Assert.IsTrue(testFile.FloatProperty == found.FloatProperty);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertWithObjectThatUsesFuncPropertyAndGetLatestWithPartition_ShouldFindMatch()
        {
            var partition = "partition";

            var testFile =
                new TestClassD
                {
                    FloatProperty = 10.0f,
                    DoubleProperty = _globalFaker.Random.Double(),
                };

            await _client.Create(testFile, partition);
            var found = await _client.GetLatest(testFile, partition);
            Assert.IsTrue(testFile.FloatProperty == found.FloatProperty);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertMultipleAndGetAll_CountShouldMatch()
        {
            var expected = 1;

            var stringValue = Guid.NewGuid().ToString();

            var testFile1 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = _globalFaker.Random.Int(),
                };

            var testFile2 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = 84
                };

            var createResult = await _client.Create(testFile1);

            createResult.OperationResult.Should().Be(ClientOperationType.Create);

            var updateResult = await _client.Update(testFile2);

            updateResult.OperationResult.Should().Be(ClientOperationType.Update);

            var found = await _client.GetSyncHistory<TestClassA>(testFile1);
            Assert.IsTrue(found.Count() == expected);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertMultipleAndGetAllWithPartition_CountShouldMatch()
        {
            var expected = 1;

            var stringValue = Guid.NewGuid().ToString();
            var partition = "partition";

            var testFile1 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = _globalFaker.Random.Int(),
                };

            var testFile2 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = 84
                };

            var createResult = await _client.Create(testFile1, partition);

            createResult.OperationResult.Should().Be(ClientOperationType.Create);

            var updateResult = await _client.Update(testFile2, partition);

            updateResult.OperationResult.Should().Be(ClientOperationType.Update);

            var found = await _client.GetSyncHistory<TestClassA>(testFile1, partition);
            Assert.IsTrue(found.Count() == expected);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertMultipleAndQuery_DoesGetLatest()
        {
            var stringValue = Guid.NewGuid().ToString();

            var testFile1 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = _globalFaker.Random.Int(),
                };

            var testFile2 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = 84
                };

            var testFile3 =
                new TestClassA
                {
                    StringProperty = stringValue,
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
        [DoNotParallelize]
        public async Task OrbitClient_InsertMultipleAndQueryWithPartition_DoesGetLatest()
        {
            var stringValue = Guid.NewGuid().ToString();

            var partition = $"partition_{stringValue}";


            var testFile1 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = _globalFaker.Random.Int(),
                };

            var testFile2 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = _globalFaker.Random.Int(),
                };

            var testFile3 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = _globalFaker.Random.Int(),
                };

            await _client.Upsert(testFile1, partition);
            await _client.Upsert(testFile2, partition);
            await _client.Upsert(testFile3, partition);

            var found = await _client.GetLatest(testFile3, partition);

            found.IntProperty.Should().Be(testFile3.IntProperty);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertAndDeleteAndInsert_ShouldNotInsert()
        {
            var stringValue = Guid.NewGuid().ToString();

            var testFile1 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = _globalFaker.Random.Int(),
                };

            var testFile2 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = 84
                };

            var testFile3 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = 168
                };

            var upsert1Result = await _client.Upsert(testFile1);

            upsert1Result.OperationResult
                .Should()
                .Be(ClientOperationType.Create);

            var deleteResult = await _client.Delete(testFile2);

            deleteResult.OperationResult
                .Should()
                .Be(ClientOperationType.Delete);

            var upsert2Result = await _client.Upsert(testFile3);

            upsert2Result.OperationResult
                .Should()
                .Be(ClientOperationType.NoOperation);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertAndDeleteAndInsertWithPartition_ShouldNotInsert()
        {
            var partition = "partition";


            var stringValue = Guid.NewGuid().ToString();

            var testFile1 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = 12312
                };

            var testFile2 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = 2345567
                };

            var testFile3 =
                new TestClassA
                {
                    StringProperty = stringValue,
                    IntProperty = 2345236
                };

            var upsert1Result = await _client.Upsert(testFile1, partition);

            upsert1Result.Success
                .Should()
                .BeTrue();

            upsert1Result.OperationResult
                .Should()
                .Be(ClientOperationType.Create);

            var deleteResult =await _client.Delete(testFile2, partition);

            deleteResult.Success
                .Should()
                .BeTrue();

            deleteResult.OperationResult
                .Should()
                .Be(ClientOperationType.Delete);

            var upsert2Result = await _client.Upsert(testFile3, partition);

            upsert2Result.Success
                .Should()
                .BeFalse();

            upsert2Result.OperationResult
                .Should()
                .Be(ClientOperationType.NoOperation);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertMultipleWithSameKey_ShouldFindRightTypes()
        {
            var id = "Test Value";

            var testFile1 =
                new TestClassA
                {
                    StringProperty = id,
                    IntProperty = _globalFaker.Random.Int(),
                };

            var testFile2 =
                new TestClassB
                {
                    StringProperty = id,
                    DoubleProperty = _globalFaker.Random.Double(),
                };
                
            await _client.Upsert(testFile1);
            await _client.Upsert(testFile2);

            var foundA = await _client.GetLatest<TestClassA>(id);
            var foundB = await _client.GetLatest<TestClassB>(id);

            Assert.IsTrue(foundA.IntProperty == testFile1.IntProperty);
            Assert.IsTrue(foundB.DoubleProperty == testFile2.DoubleProperty);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertMultipleWithSameKeyWithPartition_ShouldFindRightTypes()
        {
            var id = "Test Value";

            var partition = "partition";

            var testFile1 =
                new TestClassA
                {
                    StringProperty = id,
                    IntProperty = _globalFaker.Random.Int(),
                };

            var testFile2 =
                new TestClassB
                {
                    StringProperty = id,
                    DoubleProperty = _globalFaker.Random.Double(),
                };

            await _client.Upsert(testFile1, partition);
            await _client.Upsert(testFile2, partition);

            var foundA = await _client.GetLatest<TestClassA> (id, partition);
            var foundB = await _client.GetLatest<TestClassB> (id, partition);

            Assert.IsTrue(foundA.IntProperty == testFile1.IntProperty);
            Assert.IsTrue(foundB.DoubleProperty == testFile2.DoubleProperty);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_GetLatestSyncQueueWithInvalidId_ShouldFindNothing()
        {
            var expected = 0;
            var id = Guid.NewGuid().ToString();

            var partition = "test";

            var testFile1 =
                new TestClassB
                {
                    StringProperty = id,
                    DoubleProperty = _globalFaker.Random.Double(),
                };

            var insertSuccess = await _client.Upsert(testFile1, partition);

            insertSuccess.Success.Should().BeTrue();
            insertSuccess.OperationResult.Should().Be(ClientOperationType.Create);

            var foundA = await _client.GetAllLatestSyncQueue<TestClassB>();

            var foundCount = foundA.Count();

            foundCount.Should().Be(expected);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_GetAllLatest_PerfTest1 ()
        {

            var partition = "test";

            var items =
                Enumerable
                    .Range(1, 2000)
                    .Select(id =>
                        new ServerSyncInfo<TestClassA>
                        {
                            Value =
                                new TestClassA
                                {
                                    StringProperty = $"id_{id}",
                                    IntProperty = id
                                }
                        })
                    .ToList();

            await _client.Reconcile(items, partition);

            var sw = Stopwatch.StartNew();
            var foundA = await _client.GetAllLatest<TestClassA>(partition);
            sw.Stop();

            Console.WriteLine($"GetAllLatest: {sw.ElapsedMilliseconds}ms");

            foundA.Should().NotBeEmpty();
        }

        [TestMethod]
        [DoNotParallelize]
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
        [DoNotParallelize]
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

            var foundObject = await _client.GetLatest<TestClassA>(original);

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
        [DoNotParallelize]
        public async Task OrbitClient_BulkInsertAndUpdateWithPartition_ShouldGetNewValue()
        {

            var testObjects =
                new Faker<TestClassA>()
                    .RuleFor(x => x.StringProperty, (f, u) => $"String_{f.IndexFaker}")
                    .RuleFor(x => x.IntProperty, (f, u) => f.IndexFaker);

            var partition = "partition";

            var generatedTestObjects = testObjects.GenerateBetween(100, 100);

            var populated = await _client.PopulateCache(generatedTestObjects, partition);

            Assert.IsTrue(populated);

            var original = generatedTestObjects[49];

            var foundObject = await _client.GetLatest<TestClassA>(original, partition);

            Assert.AreEqual(foundObject.IntProperty, original.IntProperty);

            foundObject.IntProperty = foundObject.IntProperty * 2;

            var upsertResult = await _client.Upsert(foundObject, partition);

            upsertResult.Success
                .Should()
                .BeTrue();

            upsertResult.OperationResult
                .Should()
                .Be(ClientOperationType.Update);

            var latest = await _client.GetAllLatest<TestClassA>(partition);

            var updated = latest.FirstOrDefault(x => x.StringProperty == original.StringProperty);

            Assert.IsTrue(updated.IntProperty == foundObject.IntProperty);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_BulkInsertAndDelete_ShouldDeleteAll ()
        {
            var expected = 100;

            var testObjects =
                new Faker<TestClassA> ()
                    .RuleFor (x => x.StringProperty, (f, u) => $"String_{f.IndexFaker}")
                    .RuleFor (x => x.IntProperty, (f, u) => f.IndexFaker);

            var partition = "partition";

            var generatedTestObjects = testObjects.GenerateBetween (100, 100);

            var populated = await _client.PopulateCache (generatedTestObjects, partition);

            Assert.IsTrue (populated);

            var deleteCount = 0;

            foreach (var generatedTestObject in generatedTestObjects)
            {
                var deleteResult = await _client.DeleteCacheItem (generatedTestObject, partition);

                if(deleteResult == true)
                {
                    deleteCount += 1;
                }

                Assert.IsTrue (deleteResult);
            }

            Assert.AreEqual (expected, deleteCount);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_InsertItemsWithCategories_ShouldGetRightItemForPartition()
        {
            var partition1 = "partition1";
            var partition2 = "partition2";

            var testFile1 =
                new TestClassA
                {
                    StringProperty = "test1",
                    IntProperty = _globalFaker.Random.Int(),
                };

            var testFile2 =
                new TestClassA
                {
                    StringProperty = "test2",
                    IntProperty = 84
                };

            await _client.Upsert(testFile1, partition1);
            await _client.Upsert(testFile2, partition2);

            var testFile1InPartition2 = await _client.GetLatest<TestClassA>(testFile1, partition2);
            var testFile2InPartition1 = await _client.GetLatest<TestClassA>(testFile2, partition1);

            Assert.IsNull(testFile1InPartition2);
            Assert.IsNull(testFile2InPartition1);

            var testFile1InPartition1 = await _client.GetLatest<TestClassA>(testFile1, partition1);
            var testFile2InPartition2 = await _client.GetLatest<TestClassA>(testFile2, partition2);

            Assert.IsNotNull(testFile1InPartition1);
            Assert.IsNotNull(testFile2InPartition2);
        }

        //[TestMethod]
        //public async Task OrbitClient_InsertItemsWithCategories_ShouldFindCategories()
        //{
        //    var partition1 = "partition1";
        //    var partition2 = "partition2";

        //    var expectedCategories = 2;

        //    var testFile1 =
        //        new TestClassA
        //        {
        //            StringProperty = "test1",
        //            IntProperty = _globalFaker.Random.Int(),
        //        };

        //    var testFile2 =
        //        new TestClassA
        //        {
        //            StringProperty = "test2",
        //            IntProperty = 84
        //        };

        //    await _client.Upsert(testFile1, partition1);
        //    await _client.Upsert(testFile2, partition2);

        //    var categories = (await _client.GetCategories<TestClassA>()).ToList();

        //    Assert.IsTrue(categories.Contains(partition1));
        //    Assert.IsTrue(categories.Contains(partition2));
        //    Assert.IsTrue(categories.Count == expectedCategories);
        //}

        //[TestMethod]
        //public async Task OrbitClient_PopulateCacheAndInsertItemsWithCategories_ShouldFindCategories()
        //{
        //    var partition1 = "partition1";
        //    var partition2 = "partition2";

        //    var expectedCategories = 2;

        //    var testFile1 =
        //        new TestClassA
        //        {
        //            StringProperty = "test1",
        //            IntProperty = _globalFaker.Random.Int(),
        //        };

        //    var testFile2 =
        //        new TestClassA
        //        {
        //            StringProperty = "test2",
        //            IntProperty = 84
        //        };

        //    await _client.PopulateCache(new[] { testFile1 }, partition1);
        //    await _client.Upsert(testFile2, partition2);

        //    var categories = (await _client.GetCategories<TestClassA>()).ToList();

        //    Assert.IsTrue(categories.Contains(partition1));
        //    Assert.IsTrue(categories.Contains(partition2));
        //    Assert.IsTrue(categories.Count == expectedCategories);
        //}

        //[TestMethod]
        //public async Task OrbitClient_PopulateCacheWithSimpleItems_ShouldPopulate()
        //{
        //    var partition1 = "partition1";
        //    var partition2 = "partition2";

        //    await _client.PopulateCache(new[] { partition1, partition2 });

        //    var latest = await _client.GetAllLatest<string>();

        //    Assert.IsTrue(latest.Count() == 2);
        //}

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_Reconcile_ShouldGetServerValue ()
        {
            var index = 1;

            var testObjects =
                new Faker<TestClassA>()
                    .RuleFor(x => x.StringProperty, (f, u) => $"String_{index++}")
                    .RuleFor(x => x.IntProperty, (f, u) => index++)
                    .RuleFor(x => x.TimestampMillis, (f, u) => DateTimeOffset.Now.ToUnixTimeMilliseconds());

            var partition = "partition";

            var generatedTestObjects = testObjects.GenerateBetween(100, 100);

            var populated = await _client.PopulateCache(generatedTestObjects, partition);

            for (int i = 0; i < 50; i++)
            {
                await _client.Upsert(generatedTestObjects[i], partition);
            }

            Assert.IsTrue(populated);

            index = 1;

            var generatedTestServerObjects =
                testObjects
                    .GenerateBetween(100, 100)
                    .Select(x =>
                        new ServerSyncInfo<TestClassA>
                        {
                            Value = x,
                            Operation = ServerOperationType.Updated,
                        })
                    .ToList();

            await _client.Reconcile(generatedTestServerObjects, partition);

            var latest = await _client.GetAllLatest<TestClassA>(partition);

            foreach (var obj in generatedTestServerObjects)
            {
                var found = latest.FirstOrDefault(x => x.StringProperty == obj.Value.StringProperty && x.IntProperty == obj.Value.IntProperty && x.TimestampMillis == obj.Value.TimestampMillis);
                Assert.IsTrue(found != default);
            }

            var syncQueue = await _client.GetAllLatestSyncQueue<TestClassA>(partition);
            Assert.IsTrue(!syncQueue.Any());

            var allLatest = await _client.GetAllLatest<TestClassA>(partition);
            Assert.IsTrue(allLatest.Count() == 100);
        }

        [TestMethod]
        [DoNotParallelize]
        public async Task OrbitClient_SlamItWithMessagesConcurrently_ShouldBeOkay ()
        {

            var testAObjects =
                new Faker<TestClassA> ()
                    .RuleFor (x => x.StringProperty, (f, u) => $"String_{f.IndexFaker}")
                    .RuleFor (x => x.IntProperty, (f, u) => f.IndexFaker);

            var testBObjects =
                new Faker<TestClassB> ()
                    .RuleFor (x => x.StringProperty, (f, u) => $"String_{f.IndexFaker}")
                    .RuleFor (x => x.DoubleProperty, (f, u) => f.IndexFaker);

            var generatedTestAObjects = testAObjects.GenerateBetween (1000, 10000);

            var generatedTestBObjects = testAObjects.GenerateBetween (1000, 10000);

            Exception exception = null;
            for (int i = 0; i < 10; i++)
            {
                try
                {
                    var populatedATask = _client.PopulateCache (generatedTestAObjects);
                    var latestATask = _client.GetAllLatest<TestClassA> ();
                    var populatedBTask = _client.PopulateCache (generatedTestBObjects);
                    var latestBTask = _client.GetAllLatest<TestClassB> ();

                    await Task.WhenAll (populatedATask, populatedBTask, latestATask, latestBTask);
                }
                catch (Exception ex)
                {
                    exception = ex;
                }

            }

            Assert.IsNull (exception);
        }

        class TestClassA
        {
            public string StringProperty { get; set; }

            public int IntProperty { get; set; }

            public long TimestampMillis { get; set; }
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
