using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xamarin.Forms;
using EightBot.Orbit.Client;
using Splat;
using System.Net.Http;
using Newtonsoft.Json;
using OrbitSample.Models;
using System.Runtime.InteropServices;
using EightBot.Orbit;
using System.Diagnostics;

namespace OrbitSample
{
    // Learn more about making custom code visible in the Xamarin.Forms previewer
    // by visiting https://aka.ms/xamarinforms-previewer
    [DesignTimeVisible(true)]
    public partial class MainPage : ContentPage
    {
        public MainPage()
        {
            InitializeComponent();
        }

        protected override async void OnAppearing()
        {
            base.OnAppearing();

            try
            {
                this.IsBusy = true;

                var client = Locator.Current.GetService<OrbitClient>();

                var httpClient = new HttpClient();

                var usersJson = await httpClient.GetStringAsync("https://jsonplaceholder.typicode.com/users").ConfigureAwait(false);
                var users = JsonConvert.DeserializeObject<IEnumerable<User>>(usersJson);

                var postJson = await httpClient.GetStringAsync("https://jsonplaceholder.typicode.com/posts").ConfigureAwait(false);
                var posts = JsonConvert.DeserializeObject<IEnumerable<Post>>(postJson);


                var stopWatch = new Stopwatch();
                stopWatch.Start();

                System.Diagnostics.Debug.WriteLine($"Starting Populate Cache: {stopWatch.ElapsedMilliseconds}ms");
                await Task.WhenAll(client.PopulateCache(posts), client.PopulateCache(users));
                System.Diagnostics.Debug.WriteLine($"Finished Populate Cache: {stopWatch.ElapsedMilliseconds}ms");

                System.Diagnostics.Debug.WriteLine($"Starting Upsert: {stopWatch.ElapsedMilliseconds}ms");
                foreach (var user in users)
                {
                    user.Name = $"{user.Name} updated";
                    await client.Upsert(user).ConfigureAwait(false);
                }

                foreach (var post in posts)
                {
                    post.Title = $"{post.Title} updated";
                    await client.Upsert(post).ConfigureAwait(false);
                }
                System.Diagnostics.Debug.WriteLine($"Finished Upsert: {stopWatch.ElapsedMilliseconds}ms");

                System.Diagnostics.Debug.WriteLine($"Starting Synch History: {stopWatch.ElapsedMilliseconds}ms");
                var syncQueueLatestUser = await client.GetSyncHistory<User>().ConfigureAwait(false);
                var syncQueueLatestPost = await client.GetSyncHistory<Post>().ConfigureAwait(false);
                System.Diagnostics.Debug.WriteLine($"Finished Synch History: {stopWatch.ElapsedMilliseconds}ms");
                var postUserValue = JsonConvert.SerializeObject(syncQueueLatestUser);
                var postPostValue = JsonConvert.SerializeObject(syncQueueLatestPost);

                System.Diagnostics.Debug.WriteLine($"Starting Post to Sync API: {stopWatch.ElapsedMilliseconds}ms");
                var responseUsers = await httpClient.PostAsync("http://10.211.55.3:5000/api/sync/users", new StringContent(postUserValue, Encoding.UTF8, "application/json"));
                var responsePosts = await httpClient.PostAsync("http://10.211.55.3:5000/api/sync/posts", new StringContent(postPostValue, Encoding.UTF8, "application/json"));
                System.Diagnostics.Debug.WriteLine($"Finished Post to Sync API: {stopWatch.ElapsedMilliseconds}ms");

                var parsedUserResponse = JsonConvert.DeserializeObject<IEnumerable<ServerSyncInfo<User>>>(await responseUsers.Content.ReadAsStringAsync().ConfigureAwait(false));
                var parsedPostResponse = JsonConvert.DeserializeObject<IEnumerable<ServerSyncInfo<Post>>>(await responsePosts.Content.ReadAsStringAsync().ConfigureAwait(false));

                System.Diagnostics.Debug.WriteLine($"Starting Reconcile: {stopWatch.ElapsedMilliseconds}ms");
                await Task.WhenAll(client.Reconcile(parsedUserResponse), client.Reconcile(parsedPostResponse));
                System.Diagnostics.Debug.WriteLine($"Finished Reconcile: {stopWatch.ElapsedMilliseconds}ms");

                System.Diagnostics.Debug.WriteLine($"Starting Get All Latest: {stopWatch.ElapsedMilliseconds}ms");
                var latestUsers = await client.GetAllLatest<User>().ConfigureAwait(false);
                var latestPosts = await client.GetAllLatest<Post>().ConfigureAwait(false);
                System.Diagnostics.Debug.WriteLine($"Finished Get All Latest: {stopWatch.ElapsedMilliseconds}ms");

                stopWatch.Stop();

                Device.BeginInvokeOnMainThread(() => listView.ItemsSource = latestUsers);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"{ex}");
            }
            finally
            {
                this.IsBusy = false;
            }
        }
    }
}
