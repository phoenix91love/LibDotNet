using Microsoft.Data.SqlClient;
using MySql.Data.MySqlClient;
using Npgsql;
using System;
using System.Data;

namespace Libs.DBHelpers
{
    public sealed class DBConnections : IDisposable
    {
        public NpgsqlConnection? PostgresConnection { get; set; }
        public MySqlConnection? MySqlConnection { get; set; }
        public SqlConnection? SqlConnection { get; set; }

        public void Dispose()
        {
            if (this.PostgresConnection != null)
            {
                if (this.PostgresConnection.State == ConnectionState.Open)
                    this.PostgresConnection.Close();
                this.PostgresConnection.Dispose();
            }

            else if (this.MySqlConnection != null)
            {
                if (this.MySqlConnection.State == ConnectionState.Open)
                    this.MySqlConnection.Close();
                this.MySqlConnection.Dispose();
            }

            else if (this.SqlConnection != null)
            {
                if (this.SqlConnection.State == ConnectionState.Open)
                    this.SqlConnection.Close();
                this.SqlConnection.Dispose();
            }
        }
    }
    public sealed class DBCommands : IDisposable
    {
        public NpgsqlCommand? PostgresCommand { get; set; }
        public MySqlCommand? MySqlCommand { get; set; }
        public SqlCommand? SqlCommand { get; set; }
        public void Dispose()
        {
            if (this.PostgresCommand != null)
            {
                if (this.PostgresCommand.Connection.State == ConnectionState.Open)
                    this.PostgresCommand.Connection.Close();
                this.PostgresCommand.Dispose();
            }

            else if (this.MySqlCommand != null)
            {
                if (this.MySqlCommand.Connection.State == ConnectionState.Open)
                    this.MySqlCommand.Connection.Close();
                this.MySqlCommand.Dispose();
            }

            else if (this.SqlCommand != null)
            {
                if (this.SqlCommand.Connection.State == ConnectionState.Open)
                    this.SqlCommand.Connection.Close();
                this.SqlCommand.Dispose();
            }
        }
    }
    public sealed class DBTransactions
    {
        public NpgsqlTransaction? PostgresTransaction { get; set; }
        public MySqlTransaction? MySqlTransaction { get; set; }
        public SqlTransaction? SqlTransaction { get; set; }
        public void Dispose()
        {
            if (this.PostgresTransaction != null)
            {
                if (this.PostgresTransaction.Connection.State == ConnectionState.Open)
                    this.PostgresTransaction.Connection.Close();
                this.PostgresTransaction.Dispose();
            }

            else if (this.MySqlTransaction != null)
            {
                if (this.MySqlTransaction.Connection.State == ConnectionState.Open)
                    this.MySqlTransaction.Connection.Close();
                this.MySqlTransaction.Dispose();
            }

            else if (this.SqlTransaction != null)
            {
                if (this.SqlTransaction.Connection.State == ConnectionState.Open)
                    this.SqlTransaction.Connection.Close();
                this.SqlTransaction.Dispose();
            }
        }
    }
}
