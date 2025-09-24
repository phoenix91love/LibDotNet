using DapperCoreLib;
using Microsoft.Data.SqlClient;
using MySql.Data.MySqlClient;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
namespace LibDotNet.DBHelpers
{
    /// <summary>
    /// Access multiple database Sql Server
    /// </summary>
    /// <typeparam name="T">Type of DatabaseTypeBase: DatabaseSqlServer, DatabaseMySql,DatabasePostgreSql,DatabaseOracle </typeparam>
    public static class DatabaseAccess<T> where T : DatabaseTypeBase, new()
    {
        private static DbConnection CreateConnection(string connectionString)
        {
            var dbType = new T().Type;

            return dbType switch
            {
                DatabaseTypes.SqlServer => new SqlConnection(connectionString),
                DatabaseTypes.MySql => new MySqlConnection(connectionString),
                DatabaseTypes.PostgreSql => new NpgsqlConnection(connectionString),
                DatabaseTypes.Oracle => new OracleConnection(connectionString),
                _ => throw new ArgumentException($"Unsupported database type: {dbType}")
            };
        }

        public static async Task<IEnumerable<TResult>> QueryAsync<TResult>(string connectionString, string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            await using var connection = CreateConnection(connectionString);
            await connection.OpenAsync();
            return await connection.QueryAsync<TResult>(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
        }

        public static async Task<TResult?> QueryFirstOrDefaultAsync<TResult>(string connectionString, string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            await using var connection = CreateConnection(connectionString);
            await connection.OpenAsync();
            return await connection.QueryFirstOrDefaultAsync<TResult>(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
        }

        public static async Task<TResult?> QueryFirstAsync<TResult>(string connectionString, string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            await using var connection = CreateConnection(connectionString);
            await connection.OpenAsync();
            return await connection.QueryFirstAsync<TResult>(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
        }

        public static async Task<TResult?> QuerySingleOrDefaultAsync<TResult>(string connectionString, string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            await using var connection = CreateConnection(connectionString);
            await connection.OpenAsync();
            return await connection.QuerySingleOrDefaultAsync<TResult>(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
        }

        public static async Task<TResult> QuerySingleAsync<TResult>(string connectionString, string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            await using var connection = CreateConnection(connectionString);
            await connection.OpenAsync();
            return await connection.QuerySingleAsync<TResult>(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
        }

        public static async Task<int> ExecuteAsync(string connectionString, string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            await using var connection = CreateConnection(connectionString);
            await connection.OpenAsync();
            return await connection.ExecuteAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);
        }

        public static async Task<(IEnumerable<T1>, IEnumerable<T2>)> QueryMultipleAsync<T1, T2>(string connectionString, string sql, object? parameters = null,int? commandTimeout = null, CommandType? commandType = null)
        {
            await using var connection = CreateConnection(connectionString);
            await connection.OpenAsync();
            using var gridReader = await connection.QueryMultipleAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);

            var result1 = await gridReader.ReadAsync<T1>();
            var result2 = await gridReader.ReadAsync<T2>();

            return (result1, result2);
        }

        public static async Task<(IEnumerable<T1>, IEnumerable<T2>, IEnumerable<T3>)> QueryMultipleAsync<T1, T2, T3>(string connectionString, string sql, object? parameters = null,int? commandTimeout = null, CommandType? commandType = null)
        {
            await using var connection = CreateConnection(connectionString);
            await connection.OpenAsync();
            using var gridReader = await connection.QueryMultipleAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);

            var result1 = await gridReader.ReadAsync<T1>();
            var result2 = await gridReader.ReadAsync<T2>();
            var result3 = await gridReader.ReadAsync<T3>();

            return (result1, result2, result3);
        }

        public static async Task<(IEnumerable<T1>, IEnumerable<T2>, IEnumerable<T3>, IEnumerable<T4>)> QueryMultipleAsync<T1, T2, T3, T4>(string connectionString, string sql, object? parameters = null,int? commandTimeout = null, CommandType? commandType = null)
        {
            await using var connection = CreateConnection(connectionString);
            await connection.OpenAsync();
            using var gridReader = await connection.QueryMultipleAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);

            var result1 = await gridReader.ReadAsync<T1>();
            var result2 = await gridReader.ReadAsync<T2>();
            var result3 = await gridReader.ReadAsync<T3>();
            var result4 = await gridReader.ReadAsync<T4>();

            return (result1, result2, result3, result4);
        }

        public static async Task<(IEnumerable<T1>, IEnumerable<T2>, IEnumerable<T3>, IEnumerable<T4>, IEnumerable<T5>)> QueryMultipleAsync<T1, T2, T3, T4, T5>(string connectionString, string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            await using var connection = CreateConnection(connectionString);
            await connection.OpenAsync();
            using var gridReader = await connection.QueryMultipleAsync(sql, parameters, commandTimeout: commandTimeout, commandType: commandType);

            var result1 = await gridReader.ReadAsync<T1>();
            var result2 = await gridReader.ReadAsync<T2>();
            var result3 = await gridReader.ReadAsync<T3>();
            var result4 = await gridReader.ReadAsync<T4>();
            var result5 = await gridReader.ReadAsync<T5>();

            return (result1, result2, result3, result4, result5);
        }

        /// <summary>
        /// Action operation in transaction scope
        /// </summary>
        public static async Task<TResult> ExecuteInTransactionAsync<TResult>(string connectionString,Func<TransactionScope<T>, Task<TResult>> operation,IsolationLevel isolationLevel =IsolationLevel.ReadCommitted)
        {
            await using var connection = CreateConnection(connectionString);
            await connection.OpenAsync();

            await using var transaction = await connection.BeginTransactionAsync(isolationLevel);

            try
            {
                var scope = TransactionScope<T>.Create(connectionString, transaction);
                var result = await operation(scope);
                await transaction.CommitAsync();
                return result;
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }

        /// <summary>
        /// Action operation in transaction scope (void version)
        /// </summary>
        public static async Task ExecuteInTransactionAsync(string connectionString,Func<TransactionScope<T>, Task> operation,IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
        {
            await using var connection = CreateConnection(connectionString);
            await connection.OpenAsync();

            await using var transaction = await connection.BeginTransactionAsync(isolationLevel);

            try
            {
                var scope = TransactionScope<T>.Create(connectionString, transaction);
                await operation(scope);
                await transaction.CommitAsync();
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }
        }

    }
}
