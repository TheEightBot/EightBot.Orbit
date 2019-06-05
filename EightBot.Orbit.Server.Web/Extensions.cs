using EightBot.Orbit.Server.Web;
using System;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class Extensions
    {
        public static IServiceCollection AddSyncControllers(this IServiceCollection services, Action<OrbitSyncControllerFeatureProvider> y)
        {
            services.AddMvcCore(x => x.Conventions.Add(new OrbitSyncControllerRouteConvention()))
                .ConfigureApplicationPartManager(x =>
                {
                    var syncControllers = new OrbitSyncControllerFeatureProvider();

                    y.Invoke(syncControllers);

                    //syncControllers.EnsureSyncController<Models.User>();
                    //syncControllers.EnsureSyncController<Models.Post>(false);

                    x.FeatureProviders.Add(syncControllers);
                });

            return services;
        }
    }
}