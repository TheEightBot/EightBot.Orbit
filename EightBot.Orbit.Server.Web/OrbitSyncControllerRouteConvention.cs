using EightBot.Nebula.DocumentDb;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ApplicationModels;
using System;

namespace EightBot.Orbit.Server.Web
{
    public class OrbitSyncControllerRouteConvention : IControllerModelConvention
    {
        public void Apply(ControllerModel controller)
        {
            if (controller.ControllerType.IsGenericType)
            {
                var genericType = controller.ControllerType.GenericTypeArguments[0];

                controller.Selectors.Add(new SelectorModel
                {
                    AttributeRouteModel = new AttributeRouteModel(new RouteAttribute($"/api/sync/{genericType.Name.Pluralize().ToLowerInvariant()}")),
                });
            }
        }
    }
}