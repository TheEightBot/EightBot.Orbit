using Microsoft.AspNetCore.Authorization;
using System;

namespace EightBot.Orbit.Server.Web
{
    [Authorize]
    public class SyncControllerAuth<T> : SyncController<T>
    {
        public SyncControllerAuth(IOrbitDataClient dataClient) : base(dataClient)
        {

        }
    }
}