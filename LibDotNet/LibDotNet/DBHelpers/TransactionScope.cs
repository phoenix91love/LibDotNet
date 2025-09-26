using DapperCoreLib;
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

namespace LibDotNet.DBHelpers
{
    /// <summary>
    /// Access multiple database Sql Server by transaction
    /// </summary>
    /// <typeparam name="T">Type of DatabaseTypeBase: DatabaseSqlServer, DatabaseMySql,DatabasePostgreSql,DatabaseOracle </typeparam>
    public class TransactionScope<T> where T : DatabaseTypeBase, new()
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
            var dbType = new T().Type;
            return dbType switch
            {
                DatabaseTypes.SqlServer => new SqlConnection(_connectionString),
                DatabaseTypes.MySql => new MySqlConnection(_connectionString),
                DatabaseTypes.PostgreSql => new NpgsqlConnection(_connectionString),
                DatabaseTypes.Oracle => new OracleConnection(_connectionString),
                _ => throw new ArgumentException($"Unsupported database type: {dbType}")
            };
        }
        private static bool HasRefCursorParameter(object? parameters)
        {
            if (parameters is DynamicParameters dynamicParams)
            {
                // Kiểm tra xem đã có ref cursor parameter chưa
                return dynamicParams.ParameterNames.Any(name =>
                    name.ToLower().Contains("cursor") || name.ToLower().Contains("result"));
            }
            return false;
        }

        public async Task<IEnumerable<TResult>> QueryAsync<TResult>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {

            var connection = _transaction.Connection ?? CreateConnection();
            return await connection.QueryAsync<TResult>(sql, parameters, _transaction, commandTimeout: commandTimeout, commandType: commandType);
        }

        public async Task<TResult?> QueryFirstOrDefaultAsync<TResult>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                var dbType = new T().Type;
                if (new T().Type == DatabaseTypes.Oracle)
                {
                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    if (!HasRefCursorParameter(parameters))
                        dynamicParameters.Add(name: ":v_cursor", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

                    return await connection.QueryFirstOrDefaultAsync<TResult>(sql, dynamicParameters, commandTimeout: commandTimeout, commandType: commandType);
                }
                else
                    return await connection.QueryFirstOrDefaultAsync<TResult>(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async Task<TResult?> QueryFirstAsync<TResult>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                var dbType = new T().Type;
                if (new T().Type == DatabaseTypes.Oracle)
                {
                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    if (!HasRefCursorParameter(parameters))
                        dynamicParameters.Add(name: ":v_cursor", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

                    return await connection.QueryFirstAsync<TResult>(sql, dynamicParameters, commandTimeout: commandTimeout, commandType: commandType);
                }
                else
                    return await connection.QueryFirstAsync<TResult>(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async Task<TResult?> QuerySingleOrDefaultAsync<TResult>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                var dbType = new T().Type;
                if (new T().Type == DatabaseTypes.Oracle)
                {
                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    if (!HasRefCursorParameter(parameters))
                        dynamicParameters.Add(name: ":v_cursor", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

                    return await connection.QuerySingleOrDefaultAsync<TResult>(sql, dynamicParameters, commandTimeout: commandTimeout, commandType: commandType);
                }
                else
                    return await connection.QuerySingleOrDefaultAsync<TResult>(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async Task<TResult> QuerySingleAsync<TResult>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                var dbType = new T().Type;
                if (new T().Type == DatabaseTypes.Oracle)
                {
                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    if (!HasRefCursorParameter(parameters))
                        dynamicParameters.Add(name: ":v_cursor", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

                    return await connection.QuerySingleAsync<TResult>(sql, dynamicParameters, commandTimeout: commandTimeout, commandType: commandType);
                }
                else
                    return await connection.QuerySingleAsync<TResult>(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async Task<int> ExecuteAsync(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure)
        {
            var connection = _transaction.Connection ?? CreateConnection();
            return await connection.ExecuteAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
        }

        public async Task<(IEnumerable<T1>, IEnumerable<T2>)> QueryMultipleAsync<T1, T2>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                var dbType = new T().Type;
                if (new T().Type == DatabaseTypes.Oracle)
                {
                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    dynamicParameters.Add(name: ":v_cursor1", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);
                    dynamicParameters.Add(name: ":v_cursor2", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

                    using var gridReaderOracle = await connection.QueryMultipleAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);

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

        public async Task<(IEnumerable<T1>, IEnumerable<T2>, IEnumerable<T3>)> QueryMultipleAsync<T1, T2, T3>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                var dbType = new T().Type;
                if (new T().Type == DatabaseTypes.Oracle)
                {
                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    dynamicParameters.Add(name: ":v_cursor1", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);
                    dynamicParameters.Add(name: ":v_cursor2", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);
                    dynamicParameters.Add(name: ":v_cursor3", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

                    using var gridReaderOracle = await connection.QueryMultipleAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);

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

        public async Task<(IEnumerable<T1>, IEnumerable<T2>, IEnumerable<T3>, IEnumerable<T4>)> QueryMultipleAsync<T1, T2, T3, T4>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                var dbType = new T().Type;
                if (new T().Type == DatabaseTypes.Oracle)
                {
                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    dynamicParameters.Add(name: ":v_cursor1", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);
                    dynamicParameters.Add(name: ":v_cursor2", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);
                    dynamicParameters.Add(name: ":v_cursor3", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);
                    dynamicParameters.Add(name: ":v_cursor4", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

                    using var gridReaderOracle = await connection.QueryMultipleAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);

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

        public async Task<(IEnumerable<T1>, IEnumerable<T2>, IEnumerable<T3>, IEnumerable<T4>, IEnumerable<T5>)> QueryMultipleAsync<T1, T2, T3, T4, T5>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = CommandType.StoredProcedure)
        {
            try
            {
                var connection = _transaction.Connection ?? CreateConnection();
                var dbType = new T().Type;
                if (new T().Type == DatabaseTypes.Oracle)
                {
                    var dynamicParameters = new OracleDynamicParameters(parameters);
                    dynamicParameters.Add(name: ":v_cursor1", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);
                    dynamicParameters.Add(name: ":v_cursor2", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);
                    dynamicParameters.Add(name: ":v_cursor3", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);
                    dynamicParameters.Add(name: ":v_cursor4", dbType: OracleDbType.RefCursor, direction: ParameterDirection.Output);

                    using var gridReaderOracle = await connection.QueryMultipleAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);

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

        // Factory method thay thế cho extension method
        internal static TransactionScope<T> Create(string connectionString, DbTransaction transaction)
        {
            return new TransactionScope<T>(connectionString, transaction);
        }
    }

}
