using Microsoft.AspNetCore.Mvc.ApplicationParts;
using Microsoft.AspNetCore.Mvc.Controllers;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace EightBot.Orbit.Server.Web
{
    public class OrbitSyncControllerFeatureProvider : IApplicationFeatureProvider<ControllerFeature>
    {
        private Dictionary<Type, bool> ControllerTypes = new Dictionary<Type, bool>();

        public OrbitSyncControllerFeatureProvider()
        {

        }

        public OrbitSyncControllerFeatureProvider(Dictionary<Type, bool> controllerTypes)
        {
            this.ControllerTypes = controllerTypes;
        }

        public void EnsureSyncController<T>(bool? authorize = true)
        {
            this.ControllerTypes[typeof(T)] = authorize.Value;
        }

        public void PopulateFeature(IEnumerable<ApplicationPart> parts, ControllerFeature feature)
        {
            foreach (KeyValuePair<Type, bool> controllerType in this.ControllerTypes)
            {
                if (controllerType.Value)
                    feature.Controllers.Add(typeof(SyncControllerAuth<>).MakeGenericType(controllerType.Key).GetTypeInfo());
                else
                    feature.Controllers.Add(typeof(SyncController<>).MakeGenericType(controllerType.Key).GetTypeInfo());
            }
        }
    }
}