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

            var client = Locator.Current.GetService<OrbitClient>();

            var httpClient = new HttpClient();
            var usersJson = await httpClient.GetStringAsync("https://jsonplaceholder.typicode.com/users");
            var users = JsonConvert.DeserializeObject<IEnumerable<User>>(usersJson);
            await client.PopulateCache(users);

            foreach (var user in users)
            {
                user.Name = $"{user.Name} updated";
                await client.Upsert(user);
            }

            var syncQueueLatest = client.GetSyncHistory<User>();
            var postValue = JsonConvert.SerializeObject(syncQueueLatest);

            var response = await httpClient.PostAsync("https://localhost:5001/sync/users", new StringContent(postValue));

            var parsedResponse = JsonConvert.DeserializeObject<IEnumerable<ServerSyncInfo<User>>>(await response.Content.ReadAsStringAsync());

            await client.Reconcile(parsedResponse);

            var latest = await client.GetAllLatest<User>();

            listView.ItemsSource = latest;
        }
    }
}
