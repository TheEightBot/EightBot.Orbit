using System;
using Xamarin.Forms;
using Xamarin.Forms.Xaml;
using EightBot.Orbit.Client;
using Splat;
using OrbitSample.Models;

namespace OrbitSample
{
    public partial class App : Application
    {
        public App()
        {
            InitializeComponent();

            var orbitClient =
                new OrbitClient()
                    .Initialize(Xamarin.Essentials.FileSystem.AppDataDirectory)
                    .AddTypeRegistration<Post, string>(x => x.UserId)
                    .AddTypeRegistration<User, string>(x => x.Username);

            Locator.CurrentMutable.RegisterConstant<OrbitClient>(orbitClient);

            MainPage = new MainPage();
        }

        protected override void OnStart()
        {
            // Handle when your app starts
        }

        protected override void OnSleep()
        {
            // Handle when your app sleeps
        }

        protected override void OnResume()
        {
            // Handle when your app resumes
        }
    }
}
