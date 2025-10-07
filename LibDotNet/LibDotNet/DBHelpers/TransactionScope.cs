using Internal.Dapper;
using Microsoft.Data.SqlClient;
using MySql.Data.MySqlClient;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace Libs.DBHelpers
{
    /// <summary>
    /// Access multiple database Sql Server by transaction
    /// </summary>
    /// <typeparam name="T">Type of DatabaseTypeBase: DatabaseSqlServer, DatabaseMySql,DatabasePostgreSql,DatabaseOracle </typeparam>
    public class TransactionScope<TDB> where TDB : DatabaseTypeBase, new()
    {
        private readonly string _connectionString;
        private readonly DbTransaction _transaction;

        public TransactionScope(string connectionString, DbTransaction transaction)
        {
            _connectionString = connectionString;
            _transaction = transaction;
        }

        private DbConnection CreateConnection()
        {
            var dbType = new TDB().Type;
            return dbType switch
            {
                DatabaseTypes.SqlServer => new SqlConnection(_connectionString),
                DatabaseTypes.MySql => new MySqlConnection(_connectionString),
                DatabaseTypes.PostgreSql => new NpgsqlConnection(_connectionString),
                DatabaseTypes.Oracle => new OracleConnection(_connectionString),
                _ => throw new ArgumentException($"Unsupported database type: {dbType}")
            };
        }
        private bool HasRefCursorParameter(object? parameters)
        {
            if (parameters is DynamicParameters dynamicParams)
            {
                // Kiểm tra xem đã có ref cursor parameter chưa
                return dynamicParams.ParameterNames.Any(name =>
                    name.ToLower().Contains("cursor") || name.ToLower().Contains("result"));
            }
            return false;
        }

        /// <summary>
        /// Get single list data
        /// </summary>
        /// <typeparam name="T">Type data need return </typeparam>
        /// <param name="sql">Query get data or store procedure</param>
        /// <param name="parameters">Parameter with query or store procedure</param>
        /// <param name="commandTimeout">Timeout query</param>
        /// <param name="commandType">Type command query. Default is store procedure</param>
        /// <param name="cusor">Cusor with return data if use Oracle database</param>
        /// <returns>List data after query</returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure, string[]? cusor = null)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                if (new TDB().Type == DatabaseTypes.Oracle)
                {
                    var dynamicParameters = new OracleDynamicParameters(parameters);

                    cusor = cusor ?? new string[1] { "v_cursor" };
                    if (cusor.Count() != 1)
                        throw new Exception("cusor lenght is not match for query");

                    foreach (var item in cusor)
                        dynamicParameters.Add(name: $":{item}", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

                    return await connection.QueryAsync<T>(sql, dynamicParameters, commandTimeout: commandTimeout, commandType: commandType);
                }
                else
                {

                    return await connection.QueryAsync<T>(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
                }

            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Get multiple list data
        /// </summary>
        /// <typeparam name="T1">Type data need return</typeparam>
        /// <typeparam name="T2">Type data need return</typeparam>
        /// <param name="sql">Query get data or store procedure</param>
        /// <param name="parameters">Parameter with query or store procedure</param>
        /// <param name="commandTimeout">Timeout query</param>
        /// <param name="commandType">Type command query. Default is store procedure</param>
        /// <param name="cusor">Cusor with return data if use Oracle database</param>
        /// <returns>List data after query</returns>
        public async Task<(IEnumerable<T1>, IEnumerable<T2>)> QueryAsync<T1, T2>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure, string[]? cusor = null)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                if (new TDB().Type == DatabaseTypes.Oracle)
                {
                    cusor = cusor ?? new string[2] { "v_cursor1", "v_cursor2" };
                    if (cusor.Count() != 2)
                        throw new Exception($"Parameter {nameof(cusor)} lenght is not match for query");

                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    foreach (var item in cusor)
                        dynamicParameters.Add(name: $":{item}", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);


                    using var gridReaderOracle = await connection.QueryMultipleAsync(sql, dynamicParameters, commandTimeout: commandTimeout, commandType: commandType);

                    var result1 = await gridReaderOracle.ReadAsync<T1>();
                    var result2 = await gridReaderOracle.ReadAsync<T2>();

                    return (result1, result2);
                }
                else
                {

                    using var gridReaderNormal = await connection.QueryMultipleAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);

                    var result1 = await gridReaderNormal.ReadAsync<T1>();
                    var result2 = await gridReaderNormal.ReadAsync<T2>();

                    return (result1, result2);
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary>
        /// Get multiple list data
        /// </summary>
        /// <typeparam name="T1">Type data need return</typeparam>
        /// <typeparam name="T2">Type data need return</typeparam>
        /// <typeparam name="T3">Type data need return</typeparam>
        /// <param name="sql">Query get data or store procedure</param>
        /// <param name="parameters">Parameter with query or store procedure</param>
        /// <param name="commandTimeout">Timeout query</param>
        /// <param name="commandType">Type command query. Default is store procedure</param>
        /// <param name="cusor">Cusor with return data if use Oracle database</param>
        /// <returns>List data after query</returns>
        public async Task<(IEnumerable<T1>, IEnumerable<T2>, IEnumerable<T3>)> QueryAsync<T1, T2, T3>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure, string[]? cusor = null)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                if (new TDB().Type == DatabaseTypes.Oracle)
                {
                    cusor = cusor ?? new string[3] { "v_cursor1", "v_cursor2", "v_cursor3" };
                    if (cusor.Count() != 3)
                        throw new Exception($"Parameter {nameof(cusor)} lenght is not match for query");

                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    foreach (var item in cusor)
                        dynamicParameters.Add(name: $":{item}", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);


                    using var gridReaderOracle = await connection.QueryMultipleAsync(sql, dynamicParameters, commandTimeout: commandTimeout, commandType: commandType);

                    var result1 = await gridReaderOracle.ReadAsync<T1>();
                    var result2 = await gridReaderOracle.ReadAsync<T2>();
                    var result3 = await gridReaderOracle.ReadAsync<T3>();
                    return (result1, result2, result3);
                }
                else
                {

                    using var gridReaderNormal = await connection.QueryMultipleAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
                    var result1 = await gridReaderNormal.ReadAsync<T1>();
                    var result2 = await gridReaderNormal.ReadAsync<T2>();
                    var result3 = await gridReaderNormal.ReadAsync<T3>();
                    return (result1, result2, result3);
                }
            }
            catch (Exception ex)
            {
                throw;
            }

        }

        /// <summary>
        /// Get multiple list data
        /// </summary>
        /// <typeparam name="T1">Type data need return</typeparam>
        /// <typeparam name="T2">Type data need return</typeparam>
        /// <typeparam name="T3">Type data need return</typeparam>
        /// <typeparam name="T4">Type data need return</typeparam>
        /// <param name="sql">Query get data or store procedure</param>
        /// <param name="parameters">Parameter with query or store procedure</param>
        /// <param name="commandTimeout">Timeout query</param>
        /// <param name="commandType">Type command query. Default is store procedure</param>
        /// <param name="cusor">Cusor with return data if use Oracle database</param>
        /// <returns>List data after query</returns>
        public async Task<(IEnumerable<T1>, IEnumerable<T2>, IEnumerable<T3>, IEnumerable<T4>)> QueryAsync<T1, T2, T3, T4>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure, string[]? cusor = null)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                if (new TDB().Type == DatabaseTypes.Oracle)
                {
                    cusor = cusor ?? new string[4] { "v_cursor1", "v_cursor2", "v_cursor3", "v_cursor4" };
                    if (cusor.Count() != 4)
                        throw new Exception($"Parameter {nameof(cusor)} lenght is not match for query");

                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    foreach (var item in cusor)
                        dynamicParameters.Add(name: $":{item}", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

                    using var gridReaderOracle = await connection.QueryMultipleAsync(sql, dynamicParameters, commandTimeout: commandTimeout, commandType: commandType);
                    var result1 = await gridReaderOracle.ReadAsync<T1>();
                    var result2 = await gridReaderOracle.ReadAsync<T2>();
                    var result3 = await gridReaderOracle.ReadAsync<T3>();
                    var result4 = await gridReaderOracle.ReadAsync<T4>();

                    return (result1, result2, result3, result4);
                }
                else
                {
                    using var gridReaderNormal = await connection.QueryMultipleAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
                    var result1 = await gridReaderNormal.ReadAsync<T1>();
                    var result2 = await gridReaderNormal.ReadAsync<T2>();
                    var result3 = await gridReaderNormal.ReadAsync<T3>();
                    var result4 = await gridReaderNormal.ReadAsync<T4>();

                    return (result1, result2, result3, result4);
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// Get multiple list data
        /// </summary>
        /// <typeparam name="T1">Type data need return</typeparam>
        /// <typeparam name="T2">Type data need return</typeparam>
        /// <typeparam name="T3">Type data need return</typeparam>
        /// <typeparam name="T4">Type data need return</typeparam>
        /// <typeparam name="T5">Type data need return</typeparam>
        /// <param name="sql">Query get data or store procedure</param>
        /// <param name="parameters">Parameter with query or store procedure</param>
        /// <param name="commandTimeout">Timeout query</param>
        /// <param name="commandType">Type command query. Default is store procedure</param>
        /// <param name="cusor">Cusor with return data if use Oracle database</param>
        /// <returns>List data after query</returns>
        public async Task<(IEnumerable<T1>, IEnumerable<T2>, IEnumerable<T3>, IEnumerable<T4>, IEnumerable<T5>)> QueryAsync<T1, T2, T3, T4, T5>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure, string[]? cusor = null)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                if (new TDB().Type == DatabaseTypes.Oracle)
                {
                    cusor = cusor ?? new string[5] { "v_cursor1", "v_cursor2", "v_cursor3", "v_cursor4", "v_cursor5" };
                    if (cusor.Count() != 5)
                        throw new Exception($"Parameter {nameof(cusor)} lenght is not match for query");

                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    foreach (var item in cusor)
                        dynamicParameters.Add(name: $":{item}", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

                    using var gridReaderOracle = await connection.QueryMultipleAsync(sql, dynamicParameters, commandTimeout: commandTimeout, commandType: commandType);
                    var result1 = await gridReaderOracle.ReadAsync<T1>();
                    var result2 = await gridReaderOracle.ReadAsync<T2>();
                    var result3 = await gridReaderOracle.ReadAsync<T3>();
                    var result4 = await gridReaderOracle.ReadAsync<T4>();
                    var result5 = await gridReaderOracle.ReadAsync<T5>();
                    return (result1, result2, result3, result4, result5);
                }
                else
                {

                    using var gridReaderNormal = await connection.QueryMultipleAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
                    var result1 = await gridReaderNormal.ReadAsync<T1>();
                    var result2 = await gridReaderNormal.ReadAsync<T2>();
                    var result3 = await gridReaderNormal.ReadAsync<T3>();
                    var result4 = await gridReaderNormal.ReadAsync<T4>();
                    var result5 = await gridReaderNormal.ReadAsync<T5>();
                    return (result1, result2, result3, result4, result5);
                }
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary>
        /// Get First Or Default object
        /// </summary>
        /// <typeparam name="T">Object need return</typeparam>
        /// <param name="sql">Query get data or store procedure</param>
        /// <param name="parameters">Parameter with query or store procedure</param>
        /// <param name="commandTimeout">Timeout query</param>
        /// <param name="commandType">Type command query. Default is store procedure</param>
        /// <param name="cusor">Cusor with return data if use Oracle database</param>
        /// <returns>Data after query</returns>
        public async Task<T?> QueryFirstOrDefaultAsync<T>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure, string[]? cusor = null)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                if (new TDB().Type == DatabaseTypes.Oracle)
                {
                    cusor = cusor ?? new string[1] { "v_cursor1" };
                    if (cusor.Count() != 1)
                        throw new Exception($"Parameter {nameof(cusor)} lenght is not match for query");

                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    foreach (var item in cusor)
                        dynamicParameters.Add(name: $":{item}", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

                    return await connection.QueryFirstOrDefaultAsync<T>(sql, dynamicParameters, commandTimeout: commandTimeout, commandType: commandType);
                }
                else
                    return await connection.QueryFirstOrDefaultAsync<T>(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary>
        /// Get First object
        /// </summary>
        /// <typeparam name="T">Object need return</typeparam>
        /// <param name="sql">Query get data or store procedure</param>
        /// <param name="parameters">Parameter with query or store procedure</param>
        /// <param name="commandTimeout">Timeout query</param>
        /// <param name="commandType">Type command query. Default is store procedure</param>
        /// <param name="cusor">Cusor with return data if use Oracle database</param>
        /// <returns>Data after query</returns>
        public async Task<T?> QueryFirstAsync<T>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure, string[]? cusor = null)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                if (new TDB().Type == DatabaseTypes.Oracle)
                {
                    cusor = cusor ?? new string[1] { "v_cursor1" };
                    if (cusor.Count() != 1)
                        throw new Exception($"Parameter {nameof(cusor)} lenght is not match for query");

                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    foreach (var item in cusor)
                        dynamicParameters.Add(name: $":{item}", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

                    return await connection.QueryFirstAsync<T>(sql, dynamicParameters, commandTimeout: commandTimeout, commandType: commandType);
                }
                else
                    return await connection.QueryFirstAsync<T>(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary>
        /// Get Single Or Default object
        /// </summary>
        /// <typeparam name="T">Object need return</typeparam>
        /// <param name="sql">Query get data or store procedure</param>
        /// <param name="parameters">Parameter with query or store procedure</param>
        /// <param name="commandTimeout">Timeout query</param>
        /// <param name="commandType">Type command query. Default is store procedure</param>
        /// <param name="cusor">Cusor with return data if use Oracle database</param>
        /// <returns>Data after query</returns>
        public async Task<T?> QuerySingleOrDefaultAsync<T>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure, string[]? cusor = null)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                if (new TDB().Type == DatabaseTypes.Oracle)
                {
                    cusor = cusor ?? new string[1] { "v_cursor1" };
                    if (cusor.Count() != 1)
                        throw new Exception($"Parameter {nameof(cusor)} lenght is not match for query");

                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    foreach (var item in cusor)
                        dynamicParameters.Add(name: $":{item}", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

                    return await connection.QuerySingleOrDefaultAsync<T>(sql, dynamicParameters, commandTimeout: commandTimeout, commandType: commandType);
                }
                else
                    return await connection.QuerySingleOrDefaultAsync<T>(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);


            }
            catch (Exception ex)
            {
                throw;
            }
        }

        /// <summary>
        /// Execute query or store procedure
        /// </summary>
        /// <param name="parameters">Parameter with query or store procedure</param>
        /// <param name="commandTimeout">Timeout query</param>
        /// <param name="commandType">Type command query. Default is store procedure</param>
        /// <returns></returns>
        public async Task<int> ExecuteAsync(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure)
        {
            var connection = _transaction.Connection ?? CreateConnection();
            return await connection.ExecuteAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
        }

        // Factory method thay thế cho extension method
        internal static TransactionScope<TDB> Create(string connectionString, DbTransaction transaction)
        {
            return new TransactionScope<TDB>(connectionString, transaction);
        }
    }

}
