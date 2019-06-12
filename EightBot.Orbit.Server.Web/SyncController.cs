using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace EightBot.Orbit.Server.Web
{
    [Route("api/sync/[controller]")]
    [ApiController]
    public class SyncController<T> : ControllerBase
    {
        private readonly IOrbitDataClient DataClient = null;

        public SyncController(IOrbitDataClient dataClient)
        {
            this.DataClient = dataClient;
        }

        [HttpPost("")]
        public async Task<ActionResult<IEnumerable<T>>> Post([FromBody]IEnumerable<ClientSyncInfo<T>> syncables)
        {
            var payload = await this.DataClient.Sync(syncables);
            if (payload != null && payload.Count() > 0)
                return Ok(payload);
            else
                return BadRequest();
        }
    }
}