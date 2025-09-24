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

        public async Task<IEnumerable<TResult>> QueryAsync<TResult>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            var connection = _transaction.Connection ?? CreateConnection();
            return await connection.QueryAsync<TResult>(sql, parameters, _transaction, commandTimeout: commandTimeout, commandType: commandType);
        }

        public async Task<TResult?> QueryFirstOrDefaultAsync<TResult>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            var connection = _transaction.Connection ?? CreateConnection();
            return await connection.QueryFirstOrDefaultAsync<TResult>(sql, parameters, _transaction, commandTimeout: commandTimeout, commandType: commandType);
        }

        public async Task<TResult> QueryFirstAsync<TResult>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            var connection = _transaction.Connection ?? CreateConnection();
            return await connection.QueryFirstAsync<TResult>(sql, parameters, _transaction, commandTimeout: commandTimeout, commandType: commandType);
        }

        public async Task<TResult?> QuerySingleOrDefaultAsync<TResult>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            var connection = _transaction.Connection ?? CreateConnection();
            return await connection.QuerySingleOrDefaultAsync<TResult>(sql, parameters, _transaction, commandTimeout: commandTimeout, commandType: commandType);
        }

        public async Task<TResult> QuerySingleAsync<TResult>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            var connection = _transaction.Connection ?? CreateConnection();
            return await connection.QuerySingleAsync<TResult>(sql, parameters, _transaction, commandTimeout: commandTimeout, commandType: commandType);
        }

        public async Task<int> ExecuteAsync(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            var connection = _transaction.Connection ?? CreateConnection();
            return await connection.ExecuteAsync(sql, parameters, _transaction, commandTimeout: commandTimeout, commandType: commandType);
        }

        public async Task<(IEnumerable<T1>, IEnumerable<T2>)> QueryMultipleAsync<T1, T2>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            var connection = _transaction.Connection ?? CreateConnection();
            using var gridReader = await connection.QueryMultipleAsync(sql, parameters, _transaction, commandTimeout: commandTimeout, commandType: commandType);

            var result1 = await gridReader.ReadAsync<T1>();
            var result2 = await gridReader.ReadAsync<T2>();

            return (result1, result2);
        }

        public async Task<(IEnumerable<T1>, IEnumerable<T2>, IEnumerable<T3>)> QueryMultipleAsync<T1, T2, T3>(string sql, object? parameters = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            var connection = _transaction.Connection ?? CreateConnection();
            using var gridReader = await connection.QueryMultipleAsync(sql, parameters, _transaction, commandTimeout: commandTimeout, commandType: commandType);

            var result1 = await gridReader.ReadAsync<T1>();
            var result2 = await gridReader.ReadAsync<T2>();
            var result3 = await gridReader.ReadAsync<T3>();

            return (result1, result2, result3);
        }

       
        // Factory method thay thế cho extension method
        internal static TransactionScope<T> Create(string connectionString, DbTransaction transaction)
        {
            return new TransactionScope<T>(connectionString, transaction);
        }
    }

}
