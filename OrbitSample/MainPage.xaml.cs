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
                var client = Locator.Current.GetService<OrbitClient>();

                var httpClient = new HttpClient();

                var usersJson = await httpClient.GetStringAsync("https://jsonplaceholder.typicode.com/users").ConfigureAwait(false);
                var users = JsonConvert.DeserializeObject<IEnumerable<User>>(usersJson);
                await client.PopulateCache(users).ConfigureAwait(false);

                foreach (var user in users)
                {
                    user.Name = $"{user.Name} updated";
                    await client.Upsert(user).ConfigureAwait(false);
                }

                var syncQueueLatest = await client.GetSyncHistory<User>().ConfigureAwait(false);
                var postValue = JsonConvert.SerializeObject(syncQueueLatest);

                var response = await httpClient.PostAsync("http://10.211.55.3:5000/api/sync/users", new StringContent(postValue, Encoding.UTF8, "application/json")).ConfigureAwait(false);

                var parsedResponse = JsonConvert.DeserializeObject<IEnumerable<ServerSyncInfo<User>>>(await response.Content.ReadAsStringAsync().ConfigureAwait(false));

                await client.Reconcile(parsedResponse).ConfigureAwait(false);

                var latest = await client.GetAllLatest<User>().ConfigureAwait(false);

                Device.BeginInvokeOnMainThread(() => listView.ItemsSource = latest);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"{ex}");
            }
        }
    }
}
