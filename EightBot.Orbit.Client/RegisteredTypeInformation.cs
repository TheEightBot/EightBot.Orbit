using System;
using System.Linq.Expressions;
using System.Reflection;
using LiteDB;

namespace EightBot.Orbit.Client
{
    internal struct RegisteredTypeInformation
    {
        public PropertyInfo PropertyIdSelector { get; set; }

        public Delegate FuncIdSelector { get; set; }

        public bool RequiresIdMapping { get; set; }

        public string IdProperty { get; set; }

        public string TypeFullName { get; set; }

        public string TypeName { get; set; }

        public string TypeNamespace { get; set; }

        public Type ObjectType { get; set; }

        public static RegisteredTypeInformation Create<T>(string typeNameOverride = null)
        {
            var type = typeof(T);

            var rti =
                new RegisteredTypeInformation
                {
                    TypeFullName = type.FullName?.Replace('+', '_'),
                    TypeName = typeNameOverride ?? type.Name,
                    TypeNamespace = type.Namespace,
                    ObjectType = type
                };

            return rti;
        }

        public static RegisteredTypeInformation Create<T, TId>(Expression<Func<T, TId>> idSelector, string typeNameOverride = null)
        {
            if (idSelector.Body is MemberExpression mex && mex.Member is PropertyInfo pi)
            {
                var type = typeof(T);

                var rti =
                    new RegisteredTypeInformation
                    {
                        PropertyIdSelector = pi,
                        IdProperty = pi.Name,
                        TypeFullName = type.FullName?.Replace ('+', '_'),
                        TypeName = typeNameOverride ?? type.Name,
                        TypeNamespace = type.Namespace,
                        ObjectType = type
                    };

                return rti;
            }

            throw new ArgumentException($"The expression provided is not a property selector for {typeof(T).Name}", nameof(idSelector));
        }

        public static RegisteredTypeInformation Create<T, TIdSelectorType, TIdPropertyType>(Expression<Func<T, TIdSelectorType>> idSelector, Expression<Func<T, TIdPropertyType>> idProperty, string typeNameOverride = null)
        {
            if (idSelector is LambdaExpression lex && idProperty.Body is MemberExpression mex && mex.Member is PropertyInfo pi)
            {
                var compiledExpression = lex.Compile();
                var type = typeof(T);

                var rti =
                    new RegisteredTypeInformation
                    {
                        FuncIdSelector = compiledExpression,
                        IdProperty = pi.Name,
                        TypeFullName = type.FullName?.Replace ('+', '_'),
                        TypeName = typeNameOverride ?? type.Name,
                        TypeNamespace = type.Namespace,
                        ObjectType = type
                    };

                return rti;
            }

            throw new ArgumentException($"The expression provided is not a lambda expression for {typeof(T).Name}", nameof(idSelector));
        }

        public BsonValue GetId<T>(T value)
        {
            if (PropertyIdSelector != null)
                return new BsonValue(PropertyIdSelector.GetValue(value));

            return
                new BsonValue(
                    FuncIdSelector != null
                        ? ((Func<T, object>)FuncIdSelector)(value)
                        : value);
        }

        public string GetCategoryTypeName(string category = null)
        {
            return
                category != null
                    ? $"{TypeName}{OrbitClient.CategorySeparator}{category}"
                    : TypeName;
        }
    }
}
