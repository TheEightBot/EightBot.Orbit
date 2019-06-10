using EightBot.Nebula.DocumentDb;
using EightBot.Orbit.Server.Web;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using System;

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

        public static IServiceCollection AddDefaultOrbitSyncCosmosDataClient(this IServiceCollection services, string databaseUri, string authKey, string databaseId, Action<IDataClient> config, bool throwErrors = true, int? throughput = 400)
        {
            services.AddSingleton<IDataClient, DataClient>(x =>
            {
                var documentClient = new DocumentClient(new Uri(databaseUri), authKey, new ConnectionPolicy() { ConnectionMode = ConnectionMode.Gateway });
                documentClient.OpenAsync().Wait();
                documentClient.CreateDatabaseIfNotExistsAsync(new Database { Id = databaseId }, new RequestOptions() { OfferThroughput = throughput }).Wait();

                var documentDbLogger = x.GetRequiredService<ILoggerFactory>().CreateLogger("EightBot.Nebula.DocumentDb");

                var dataClient = new DataClient(documentClient, databaseId, x.GetService<IHttpContextAccessor>()?.HttpContext.User)
                {
                    ThrowErrors = throwErrors,
                    LogError = y => documentDbLogger?.LogError(y),
                    LogInformation = y => documentDbLogger?.LogInformation(y)
                };

                config.Invoke(dataClient);

                return dataClient;
            });

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