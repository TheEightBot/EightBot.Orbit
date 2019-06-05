using System;
using System.Runtime.Serialization;

namespace EightBot.Orbit.Client
{
    public class ClientNotInitializedException : Exception
    {
        public ClientNotInitializedException()
        {
        }

        public ClientNotInitializedException(string message) : base(message)
        {
        }

        public ClientNotInitializedException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected ClientNotInitializedException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
