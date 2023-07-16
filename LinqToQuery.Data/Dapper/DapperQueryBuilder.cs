using Dapper.FluentMap;
using Dapper.FluentMap.Mapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace LinqToQuery.Data.Dapper
{
    public static class DapperQueryBuilder
    {
        /// <summary>
                /// Retorna uma cláusula WHERE para uma consulta SQL baseado em uma expressão lambda de filtro.
                /// Utiliza a biblioteca FluentMapper para obter o mapeamento de entidade e colunas.
                /// </summary>
                /// <typeparam name="T">O tipo da entidade a ser filtrada</typeparam>
                /// <param name="predicate">A expressão lambda que especifica o filtro para busca de dados.</param>
                /// <returns>Uma string contendo a cláusula WHERE da consulta SQL, ou uma string vazia se a expressão de filtro for nula.</returns>
        public static string GetWhereQuery<T>(Expression<Func<T, bool>> predicate) where T : class
        {
            if (predicate == null)
            {
                return string.Empty;
            }
            // Recupera o mapeamento de entidade correspondente a T através do FluentMapper.
            var entityMap = (EntityMap<T>)FluentMapper.EntityMaps.FirstOrDefault(s => s.Key.Name == typeof(T).Name).Value;
            if (entityMap == null)
            {
                return string.Empty;
            }

            // Instancia um objeto SqlExpressionVisitor<T> passando o entityMap como argumento.
            // Este objeto é responsável por percorrer a expressão lambda contida em predicate e construir a cláusula WHERE correspondente. 
            var visitor = new SqlExpressionVisitor<T>(entityMap);
            visitor.Visit(predicate);

            if (visitor.SqlWhere.Length > 0)
            {
                return $"WHERE {visitor.SqlWhere}";
            }

            return string.Empty;
        }

        /// <summary>
                /// Retorna uma cláusula ORDER BY para uma consulta SQL baseado em uma expressão lambda de ordenação.
                /// Utiliza a biblioteca FluentMapper para obter o mapeamento de entidade e colunas.
                /// </summary>
                /// <typeparam name="T">O tipo da entidade a ser ordenada</typeparam>
                /// <param name="orderBy">A expressão lambda que especifica a ordenação dos dados.</param>
                /// <returns>Uma string contendo a cláusula ORDER BY da consulta SQL, ou uma string vazia se a expressão de ordenação for nula.</returns>
        private static string GetOrderByQuery<T>(Expression<Func<T, object>> orderBy) where T : class
        {
            if (orderBy == null)
            {
                return string.Empty;
            }

            var entityMap = (EntityMap<T>)FluentMapper.EntityMaps.FirstOrDefault(s => s.Key.Name == typeof(T).Name).Value;

            // Obtém o nome da coluna correspondente à propriedade mapeada que foi passada como expressão lambda para o parâmetro orderBy.
            // Para isso, é utilizada a propriedade Body da expressão lambda, que contém a expressão que representa a propriedade.
            // Em seguida, é recuperado o nome da propriedade através da propriedade Member.Name, e então é recuperada a coluna correspondente através do entityMap.
            var column = entityMap.PropertyMaps.Single(x => x.PropertyInfo.Name == ((MemberExpression)orderBy.Body).Member.Name);
            var columnName = column.ColumnName;

            return $"ORDER BY {columnName}";
        }

        /// <summary>
                /// Obtém a consulta SQL de atualização com base no predicado e nos valores de atualização fornecidos.
                /// </summary>
                /// <typeparam name="T">O tipo da entidade.</typeparam>
                /// <param name="predicate">O predicado para selecionar os registros a serem atualizados.</param>
                /// <param name="updateValues">O objeto contendo os valores de atualização.</param>
                /// <param name="onlyUpdatedProperties">Indica se apenas as propriedades com valores de atualização devem ser incluídas na cláusula SET.</param>
                /// <returns>A consulta SQL de atualização.</returns>
        public static string GetUpdateQuery<T>(Expression<Func<T, bool>> predicate, object updateValues, string tableName, bool onlyUpdatedProperties = true) where T : class
        {
            // Verifica se o predicado ou os valores de atualização são nulos
            if (predicate == null || updateValues == null)
            {
                return string.Empty;
            }

            // Obtém o mapeamento da entidade
            var entityMap = (EntityMap<T>)FluentMapper.EntityMaps.FirstOrDefault(s => s.Key.Name == typeof(T).Name).Value;
            if (entityMap == null)
            {
                return string.Empty;
            }

            // Cria o visitante para a expressão SQL
            var visitor = new SqlExpressionVisitor<T>(entityMap);
            visitor.Visit(predicate);

            // Monta a cláusula SET da consulta SQL
            var setClause = string.Join(", ", updateValues.GetType().GetProperties()
         .Where(prop => prop.GetValue(updateValues) != null && !IsDefaultValue(prop.GetValue(updateValues))) // Filtra propriedades com valores não nulos e não padrão
                  .Select(prop =>
                  {
                      var columnName = entityMap.PropertyMaps.FirstOrDefault(map => map.PropertyInfo.Name == prop.Name)?.ColumnName;
                      if (columnName != null)
                      {
                          var value = prop.GetValue(updateValues);
                          if (value is string)
                          {
                              value = $"'{value}'";
                          }
                          else if (value is DateTime)
                          {
                              value = $"'{((DateTime)value):yyyy-MM-dd HH:mm:ss.fff}'";
                          }
                          return $"{columnName} = {value}";
                      }
                      return null;
                  })
         .Where(prop => prop != null));

            // Verifica se a opção onlyUpdatedProperties está habilitada e se a cláusula SET está vazia
            if (onlyUpdatedProperties && string.IsNullOrEmpty(setClause))
            {
                return string.Empty;
            }

            // Obtém a cláusula WHERE do visitante
            var whereClause = visitor.SqlWhere;

            // Monta a consulta SQL de atualização completa
            return $"UPDATE {tableName} SET {setClause} WHERE {whereClause}";
        }

        // Método auxiliar para verificar se o valor é o padrão para o tipo de dados
        private static bool IsDefaultValue(object value)
        {
            if (value == null)
                return true;

            Type type = value.GetType();

            if (type.IsValueType && Nullable.GetUnderlyingType(type) == null)
                return value.Equals(Activator.CreateInstance(type));

            return false;
        }
    }
    /// <summary>
        /// Visitante de expressão lambda que gera uma cláusula WHERE para consulta SQL baseado na expressão.
        /// </summary>
        /// <typeparam name="T">O tipo de entidade referenciado na expressão lambda.</typeparam>
    public class SqlExpressionVisitor<T> : ExpressionVisitor where T : class
    {
        private readonly EntityMap<T> _entityMap;
        private readonly Dictionary<string, string> _columnMappings;

        public SqlExpressionVisitor(EntityMap<T> entityMap)
        {
            _entityMap = entityMap;
            _columnMappings = new Dictionary<string, string>();

            foreach (var propertyMap in _entityMap.PropertyMaps)
            {
                _columnMappings.Add(propertyMap.PropertyInfo.Name, propertyMap.ColumnName);
            }
        }


        public string SqlWhere { get; set; } = string.Empty;  /// Obtém ou define a cláusula WHERE da consulta SQL gerada pela visita da expressão lambda.
        private string _lastVisitedMemberName; /// Cria um novo visitante de expressão lambda para gerar cláusulas WHERE SQL.
        private string _lastVisitedMemberValue; /// Visita um nó binário na expressão lambda e gera a cláusula WHERE correspondente.
        private string _lastVisitedCoalesceValue;
        private bool isCoalesce = false; /// Visita um nó binário na expressão lambda e gera a cláusula WHERE correspondente.

                                         /// <summary>
                                                                                  /// Visita um nó constante na expressão lambda e gera a cláusula WHERE correspondente.
                                                                                  /// </summary>
                                                                                  /// <param name="node">O nó constante a ser visitado.</param>
                                                                                  /// <returns>O próprio nó constante visitado.</returns>
        protected override Expression VisitBinary(BinaryExpression node)
        {
            if (node.NodeType == ExpressionType.Coalesce)
            {
                Visit(node.Left);
                var leftMemberName = _lastVisitedMemberName;

                // Visit the right side of the null coalescing operator
                Visit(node.Right);
                var rightValue = _lastVisitedMemberValue;

                // If the property is mapped in the EntityMap, create a corresponding SQL expression using COALESCE function
                if (_columnMappings.ContainsKey(leftMemberName))
                {
                    var columnName = _columnMappings[leftMemberName];
                    _lastVisitedCoalesceValue = $"COALESCE({columnName}, {rightValue})";
                    isCoalesce = true;
                    return node;
                }
            }
            // Se o operador binário for uma conjunção AndAlso ou disjunção OrElse:
            else if (node.NodeType == ExpressionType.AndAlso || node.NodeType == ExpressionType.OrElse)
            {
                // Visita recursivamente o operando esquerdo e armazena a condição em leftCondition.
                var leftSqlWhere = SqlWhere;
                Visit(node.Left);
                var leftCondition = SqlWhere;
                SqlWhere = leftSqlWhere;

                // Visita recursivamente o operando direito e armazena a condição em rightCondition.
                var rightSqlWhere = SqlWhere;
                Visit(node.Right);
                var rightCondition = SqlWhere;
                SqlWhere = rightSqlWhere;

                var op = GetOperator(node.NodeType);

                // Constrói a cláusula WHERE com as subexpressões, o operador e os parênteses.
                SqlWhere = $"{(SqlWhere.Length > 0 ? SqlWhere + " " : "")}({leftCondition} {op} {rightCondition})";
            }
            else
            {
                // Visita recursivamente o operando esquerdo e armazena o nome da propriedade em leftMemberName.
                Visit(node.Left);
                var leftMemberName = _lastVisitedMemberName;
                var rightSqlWhere = SqlWhere;

                // Visita recursivamente o operando direito e armazena o valor em rightValue.
                Visit(node.Right);
                var rightValue = _lastVisitedMemberValue;
                SqlWhere = rightSqlWhere;

                var op = GetOperator(node.NodeType);

                // Se a propriedade correspondente ao operando esquerdo estiver mapeada no objeto EntityMap através da variável _columnMappings:
                if (_columnMappings.ContainsKey(leftMemberName))
                {
                    // Obtém o nome da coluna correspondente à propriedade
                    var columnName = _columnMappings[leftMemberName];
                    if (rightValue.Equals("NULL"))
                    {
                        //No postgree operador IS é usado para comparação com NULL
                        op = "IS";
                    }
                    if (isCoalesce)
                    {
                        SqlWhere = $"{(SqlWhere.Length > 0 ? SqlWhere + " AND " : "")}{_lastVisitedCoalesceValue} {op} {rightValue}";
                        isCoalesce = false;
                    }
                    else
                    {
                        SqlWhere = $"{(SqlWhere.Length > 0 ? SqlWhere + " AND " : "")}{columnName} {op} {rightValue}";
                    }
                }

            }

            return node;
        }

        /// <summary>
                /// Visita um nó constante na expressão lambda e gera a cláusula WHERE correspondente.
                /// </summary>
                /// <param name="node">O nó constante a ser visitado.</param>
                /// <returns>O próprio nó constante visitado.</returns>
                /// <remarks>
                /// Se o tipo da constante for um valor nulo ou referência de objeto, a cláusula WHERE será definida como a própria constante.
                /// Caso contrário, a cláusula WHERE será definida como a string resultante da conversão da constante em seu valor de string equivalente.
                /// </remarks>
        protected override Expression VisitConstant(ConstantExpression node)
        {
            // Se o tipo da constante for um tipo de valor ou uma string:
            if (node.Type.IsValueType || node.Type == typeof(string))
            {
                var isString = node.Value is string;
                var isDateTime = node.Value is DateTime;

                if (isString)
                {
                    _lastVisitedMemberValue = $"'{node.Value}'";
                }
                else if (isDateTime)
                {
                    _lastVisitedMemberValue = $"'{(DateTime)node.Value:yyyy-MM-dd HH:mm:ss.fff}'";
                }
                else if (node.Value == null && !string.IsNullOrEmpty(_lastVisitedMemberValue))
                {
                    _lastVisitedMemberValue = "NULL";
                }
                else
                {
                    _lastVisitedMemberValue = node.Value?.ToString();
                }
            }
            else
            {
                // Armazena o valor da constante diretamente na variável SqlWhere.
                // Isso é feito porque a constante não será usada na cláusula WHERE - apenas propriedades e variáveis serão usadas.
                SqlWhere = _lastVisitedMemberValue;
            }

            return node;
        }

        /// <summary>
                /// Visita um nó membro na expressão lambda e gera a cláusula WHERE correspondente.
                /// </summary>
                /// <param name="node">O nó membro a ser visitado.</param>
                /// <returns>O próprio nó membro visitado.</returns>
        protected override Expression VisitMember(MemberExpression node)
        {
            // Se a expressão pai for uma expressão de parâmetro:
            if (node.Expression is ParameterExpression)
            {
                // Armazena o nome da propriedade na variável _lastVisitedMemberName.
                _lastVisitedMemberName = node.Member.Name;
                return node;
            }
            else
            {
                object memberValue;
                // Se o membro for uma propriedade e seu tipo for um enum:
                if (node.Member is PropertyInfo propertyInfo && propertyInfo.PropertyType.IsEnum)
                {
                    _lastVisitedMemberName = $"{propertyInfo.Name}.{node.Member.Name}";
                    // Compila a expressão e obtém o valor do membro.
                    memberValue = Expression.Lambda(node).Compile().DynamicInvoke();
                }
                else
                {
                    // Converte a expressão do membro em uma expressão que retorna um objeto e cria um lambda expression para compilá-la e invocá-la.
                    var memberExpression = Expression.Convert(node, typeof(object));
                    var lambda = Expression.Lambda<Func<T, object>>(memberExpression, Expression.Parameter(typeof(T)));
                    memberValue = lambda.Compile().Invoke(default);
                }

                var isString = memberValue is string;
                var isDateTime = memberValue is DateTime;

                if (isString)
                {
                    _lastVisitedMemberValue = $"'{memberValue}'";
                }
                else if (isDateTime)
                {
                    _lastVisitedMemberValue = $"'{(DateTime)memberValue:yyyy-MM-dd HH:mm:ss.fff}'";
                }
                else
                {
                    _lastVisitedMemberValue = memberValue?.ToString();
                }

                // Retorna o nó que representa o acesso à propriedade.
                return node;
            }
        }

        /// <summary>
                /// Retorna o operador SQL correspondente ao tipo de operação binária especificado na expressão lambda.
                /// </summary>
                /// <param name="nodeType">O tipo de operação binária.</param>
                /// <returns>A string que representa o operador SQL correspondente.</returns>
        private string GetOperator(ExpressionType nodeType)
        {
            switch (nodeType)
            {
                case ExpressionType.Equal:
                    return "=";
                case ExpressionType.NotEqual:
                    return "<>";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.AndAlso:
                    return "AND";
                case ExpressionType.OrElse:
                    return "OR";
                default:
                    throw new NotSupportedException($"Expression type '{nodeType}' not supported.");
            }
        }
    }
}

