using AspNetCore.RouteAnalyzer;
using EightBot.Orbit.Server;
using EightBot.Orbit.Server.Data;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace OrbitSample.Server.Web
{
    public class Startup
    {
        protected IConfiguration Configuration { get; }

        protected IHostingEnvironment HostingEnvironment { get; }

        protected IServiceProvider ServiceProvider { get; set; }

        public Startup(IConfiguration configuration, IHostingEnvironment hostingEnvironment, IServiceProvider serviceProvider)
        {
            this.Configuration = configuration;
            this.HostingEnvironment = hostingEnvironment;
            this.ServiceProvider = serviceProvider;
        }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddLogging(x =>
            {
                x.ClearProviders();

                if (this.HostingEnvironment.IsDevelopment())
                    x.AddDebug();
            });

            services.AddAuthentication(x =>
            {
                x.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
                x.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
            }).AddJwtBearer();

            services.AddMvc();

            var cosmosUri = this.Configuration.GetConnectionString("OrbitSampleAzureCosmosEndpointUri");
            var cosmosAuhKey = this.Configuration.GetConnectionString("OrbitSampleAzureCosmosAuthKey");
            var cosmosDataBaseId = this.Configuration.GetConnectionString("OrbitSampleAzureCosmosDataBaseId");

            // TODO: Clean this up. Find better async await ConfigureServices
            services.AddDefaultOrbitSyncCosmosDataClient(cosmosUri, cosmosAuhKey, cosmosDataBaseId, x =>
            {
                Task.Run(async () =>
                {
                    await x.EnsureCollectionAsync<Models.User>(y=> y.Username, y => y.Company.Name);
                    await x.EnsureCollectionAsync<Models.Post>(y => y.UniqueId, y => y.UserId);
                }).Wait();
            });

            services.AddOrbitSyncControllers(x =>
            {
                x.EnsureSyncController<Models.User>();
                x.EnsureSyncController<Models.Post>(false);
            });

            services.AddRouteAnalyzer();
        }

        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            if (env.IsDevelopment() || env.IsEnvironment("Dev"))
                app.UseDeveloperExceptionPage();
            else
            {
                app.UseHttpsRedirection();
                app.UseExceptionHandler("/Home/Error");
            }

            app.UseAuthentication();
            app.UseDefaultFiles();
            app.UseHsts();
            app.UseStaticFiles();

            app.UseMvc(routes =>
            {
                routes.MapRouteAnalyzer("/routes");
            });
        }
    }
}