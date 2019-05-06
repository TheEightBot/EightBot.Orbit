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
            client.PopulateCache(users);

            var cachedUsers = client.GetLatest<User>();

            listView.ItemsSource = cachedUsers;
        }
    }
}
