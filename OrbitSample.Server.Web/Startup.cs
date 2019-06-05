using AspNetCore.RouteAnalyzer;
using EightBot.Nebula.DocumentDb;
using EightBot.Orbit.Server;
using EightBot.Orbit.Server.Data;
using EightBot.Orbit.Server.Web;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;

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

            services.AddResponseCompression();

            ////services.AddMvc();

            //services.AddMvc(x => x.Conventions.Add(new OrbitSyncControllerRouteConvention()))
            //    .ConfigureApplicationPartManager(x =>
            //    {
            //        var syncControllers = new OrbitSyncControllerFeatureProvider();

            //        syncControllers.EnsureSyncController<Models.User>();
            //        syncControllers.EnsureSyncController<Models.Post>(false);

            //        x.FeatureProviders.Add(syncControllers);
            //    });
            ///*.SetCompatibilityVersion(CompatibilityVersion.Version_2_2)*/

            services.AddMvc();

            services.AddAntiforgery(x => x.SuppressXFrameOptionsHeader = true);

            services.AddHttpClient();

            // EightBot.Nebula
            services.AddSingleton<IDataClient, DataClient>(x =>
            {
                var cosmosDataBaseId = this.Configuration.GetConnectionString("OrbitSampleAzureCosmosDataBaseId");

                var documentClient = new DocumentClient(new Uri(this.Configuration.GetConnectionString("OrbitSampleAzureCosmosEndpointUri")), this.Configuration.GetConnectionString("OrbitSampleAzureCosmosAuthKey"), new ConnectionPolicy() { ConnectionMode = ConnectionMode.Gateway });
                documentClient.OpenAsync().Wait();
                documentClient.CreateDatabaseIfNotExistsAsync(new Database { Id = cosmosDataBaseId }, new RequestOptions() { OfferThroughput = 400 }).Wait();

                var documentDbLogger = x.GetRequiredService<ILoggerFactory>().CreateLogger("EightBot.Nebula.DocumentDb");

                var dataClient = new DataClient(documentClient, cosmosDataBaseId, x.GetService<IHttpContextAccessor>()?.HttpContext.User)
                {
                    ThrowErrors = this.HostingEnvironment.IsDevelopment(),
                    LogError = y => documentDbLogger.LogError(y),
                    LogInformation = y => documentDbLogger.LogInformation(y)
                };

                dataClient.EnsureDocumentCollection<Models.User>(y => y.Company.Name).Wait();
                dataClient.EnsureDocumentCollection<Models.Post>(y => y.UserId).Wait();

                return dataClient;
            });

            services.AddSyncControllers(x =>
            {
                x.EnsureSyncController<Models.User>();
                x.EnsureSyncController<Models.Post>(false);
            });

            services.AddSingleton<IOrbitDataClient, OrbitCosmosClient>();

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
            app.UseResponseCompression();
            app.UseStaticFiles();

            app.UseMvc(routes =>
            {
                routes.MapRouteAnalyzer("/routes");
            });
        }
    }
}