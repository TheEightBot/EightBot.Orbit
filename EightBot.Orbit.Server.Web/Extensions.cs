using EightBot.Nebula.DocumentDb;
using EightBot.Orbit.Server;
using EightBot.Orbit.Server.Data;
using EightBot.Orbit.Server.Web;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class Extensions
    {
        public static IServiceCollection AddOrbitSyncControllers(this IServiceCollection services, Action<OrbitSyncControllerFeatureProvider> config)
        {
            services.AddMvcCore(x => x.Conventions.Add(new OrbitSyncControllerRouteConvention())).ConfigureApplicationPartManager(x =>
            {
                var syncControllers = new OrbitSyncControllerFeatureProvider();

                config.Invoke(syncControllers);

                x.FeatureProviders.Add(syncControllers);
            });

            return services;
        }

        public static IServiceCollection AddDefaultOrbitSyncCosmosDataClient(this IServiceCollection services, string endpointUri, string authKey, string databaseId, Action<IDataClient> config, bool throwErrors = true, int? throughput = 400)
        {
            services.AddSingleton(x => new CosmosClient(endpointUri, authKey));

            services.AddSingleton<IDataClient, DataClient>(x =>
            {
                var documentDbLogger = x.GetRequiredService<ILoggerFactory>().CreateLogger("EightBot.Nebula.DocumentDb");

                var comosClient = x.GetRequiredService<CosmosClient>();

                var database = comosClient.CreateDatabaseIfNotExistsAsync(databaseId, throughput).Result;

                var dataClient = new DataClient(database, () => Thread.CurrentPrincipal?.Identity?.Name ?? "test")
                {
                    ThrowErrors = throwErrors,
                    LogError = y => documentDbLogger.LogError(y),
                    LogInformation = y => documentDbLogger.LogInformation(y)
                };

                config.Invoke(dataClient);

                return dataClient;
            });

            services.AddSingleton<IOrbitDataClient, OrbitCosmosDataClient>();

            return services;
        }


        //public static IServiceCollection AddDefaultOrbitSync(this IServiceCollection services, string databaseUri, string authKey, string databaseId, Action<IDataClient> config, bool throwErrors = true, int? throughput = 400)
        //{
        //    services.AddSingleton<IDataClient, DataClient>(x =>
        //    {
        //        var documentClient = new DocumentClient(new Uri(databaseUri), authKey, new ConnectionPolicy() { ConnectionMode = ConnectionMode.Gateway });
        //        documentClient.OpenAsync().Wait();
        //        documentClient.CreateDatabaseIfNotExistsAsync(new Database { Id = databaseId }, new RequestOptions() { OfferThroughput = throughput }).Wait();

        //        var documentDbLogger = x.GetRequiredService<ILoggerFactory>().CreateLogger("EightBot.Nebula.DocumentDb");

        //        var dataClient = new DataClient(documentClient, databaseId, x.GetService<IHttpContextAccessor>()?.HttpContext.User)
        //        {
        //            ThrowErrors = throwErrors,
        //            LogError = y => documentDbLogger?.LogError(y),
        //            LogInformation = y => documentDbLogger?.LogInformation(y)
        //        };

        //        //config.Invoke(dataClient);
        //        dataClient.EnsureCollectionAsync<Models.Post>(false);

        //        return dataClient;
        //    });

        //    services.AddMvcCore(x => x.Conventions.Add(new OrbitSyncControllerRouteConvention())).ConfigureApplicationPartManager(x =>
        //    {
        //        var syncControllers = new OrbitSyncControllerFeatureProvider();

        //        //config.Invoke(syncControllers);
        //        syncControllers.EnsureSyncController<Models.Post>(false);

        //        x.FeatureProviders.Add(syncControllers);
        //    });

        //    return services;
        //}
    }
}