using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using EightBot.Orbit.Client;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Threading;

namespace EightBot.Orbit.Tests
{
    [TestClass]
    public class ProcessingQueueTests
    {
        [TestMethod]
        public async Task ProcessingQueue_QueueAction_ShouldBeSuccessful()
        {
            var processingQueue = new ProcessingQueue();

            await processingQueue.Queue(() => { });
        }

        [TestMethod]
        public async Task ProcessingQueue_QueueFunc_ShouldBeSuccessful()
        {
            var processingQueue = new ProcessingQueue();

            var result = await processingQueue.Queue(() => { return true; });

            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task ProcessingQueue_QueueMultiple_ShouldFinishInOrder()
        {
            var processingQueue = new ProcessingQueue();

            var count = 0;

            var results = new List<Task<int>>();

            for (int i = 0; i < 100; i++)
            {
                results.Add(processingQueue.Queue(() => { return Interlocked.Increment(ref count); }));
            }

            await Task.WhenAll(results);

            for (int i = 1; i < results.Count; i++)
            {
                var resultCurr = await results[i];
                var resultPrev = await results[i - 1];

                Assert.IsTrue(resultCurr == resultPrev + 1);
            }
        }

        [TestMethod]
        public async Task ProcessingQueue_QueueMultipleWithDifferingProcessingTimes_ShouldFinishInOrder()
        {
            var processingQueue = new ProcessingQueue();

            var count = 0;

            var results = new List<Task<int>>();

            var rng = new Random(12345);

            for (int i = 0; i < 100; i++)
            {
                results.Add(
                    processingQueue
                        .Queue(
                            async () => 
                            {
                                await Task.Delay(rng.Next(10, 100));
                                return Interlocked.Increment(ref count); 
                            }));
            }

            await Task.WhenAll(results);

            for (int i = 1; i < results.Count; i++)
            {
                var resultCurr = await results[i];
                var resultPrev = await results[i - 1];

                Assert.IsTrue(resultCurr == resultPrev + 1);
            }
        }
    }
}
