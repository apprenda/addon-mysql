using Apprenda.SaaSGrid.Addons;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Data;
using MySql.Data.MySqlClient;
using Apprenda.Services.Logging;

namespace Apprenda.MySQL.AddOn
{
    public class MySQLAddOn : AddonBase
    {
        const string ConnectionStringFormatter = @"Server={0};Port={1};Uid={2};Pwd={3};Database={4};";
        const string DatabaseNameFormatter = @"{0}__{1}";
        const string DatabaseUsernameFormatter = @"'DB_{0}__{1}'@'%'";

        private static readonly ILogger log = LogManager.Instance().GetLogger(typeof(MySQLAddOn));
        
        static class Queries
        {
            public const string CreateUser = @"CREATE USER {0} IDENTIFIED BY '{1}';";
            public const string CreateDatabase = @"CREATE DATABASE {0};";
            public const string GrantAllPrivilegesToDatabase = @"GRANT ALL ON {0}.* TO {1};";
            public const string DropDatabase = @"DROP DATABASE IF EXISTS {0};";
            public const string DropUser = @"DROP USER IF EXISTS {0}";
        }

        static class Keys 
        {
            public const string Server = "mysqlServer";
            public const string Port = "mysqlServerPort";
            public const string AdminDatabase = "mysqlAdminDatabase";
            public const string AdminUser = "mysqlAdminUser";
            public const string AdminPassword = "mysqlAdminPassword";
        }

        private string Server { get; set; }
        private int Port { get; set; }
        private string AdminDatabase { get; set; }
        private string AdminUserId { get; set; }
        private string AdminPassword { get; set; }
        private string AdminConnectionString { get { return string.Format(ConnectionStringFormatter, Server, Port, AdminUserId, AdminPassword, AdminDatabase); } }
        private string NewDatabase { get; set; }
        private string NewUserId { get; set; }
        private string NewPassword { get; set; }
        private string NewConnectionString { get { return string.Format(ConnectionStringFormatter, Server, Port, NewUserId, NewPassword, NewDatabase); } }
        public override OperationResult Deprovision(AddonDeprovisionRequest request)
        {
            var result = new OperationResult();
            try
            {
                NewDatabase = GetDatabaseName(request.Manifest);
                NewUserId = GetNewUsername(request.Manifest);

                log.InfoFormat("Removing MySQL database: {0}", NewDatabase);

                using (var connection = GetConnection(request.Manifest.Properties))
                {
                    DropDatabase(connection);

                    DropUser(connection);
                }

                result.IsSuccess = true;
                result.EndUserMessage = "Successfully removed a MySQL database.";

                log.InfoFormat("Successfully removed MySQL database: {0}", NewDatabase);
            }
            catch (Exception ex)
            {
                result.IsSuccess = false;
                result.EndUserMessage = ex.Message;

                log.ErrorFormat("Failed to remove MySQL database '{0}': {1}", NewDatabase, ex.Message);
                log.Error(ex.StackTrace);
            }

            return result;
        }

        public override ProvisionAddOnResult Provision(AddonProvisionRequest request)
        {
            var result = new ProvisionAddOnResult();
            try
            {
                NewDatabase = GetDatabaseName(request.Manifest);
                NewUserId = GetNewUsername(request.Manifest);
                NewPassword = GetPassword();

                log.InfoFormat("Creating MySQL database: {0}", NewDatabase);

                using (var connection = GetConnection(request.Manifest.Properties))
                {
                    CreateUser(connection);

                    CreateDatabase(connection);

                    GrantPrivileges(connection);
                }

                result.IsSuccess = true;
                result.ConnectionData = NewConnectionString;
                result.EndUserMessage = "Successfully created a MySQL database.";

                log.InfoFormat("Successfully created MySQL database: {0}", NewDatabase);
            }
            catch (Exception ex)
            {
                result.IsSuccess = false;
                result.ConnectionData = "";
                result.EndUserMessage = ex.Message;

                log.ErrorFormat("Failed to create MySQL database '{0}': {1}", NewDatabase, ex.Message);
                log.Error(ex.StackTrace);
            }

            return result;
        }

        public override OperationResult Test(AddonTestRequest request)
        {
            var result = new OperationResult();

            try
            {
                NewDatabase = GetDatabaseName(request.Manifest);
                NewUserId = GetNewUsername(request.Manifest);
                NewPassword = GetPassword();

                log.InfoFormat("Creating and removing MySQL database: {0}", NewDatabase);

                using (var connection = GetConnection(request.Manifest.Properties))
                {
                    CreateUser(connection);

                    CreateDatabase(connection);

                    GrantPrivileges(connection);

                    DropDatabase(connection);

                    DropUser(connection);
                }

                result.IsSuccess = true;
                result.EndUserMessage = "Successfully created and removed a MySQL database.";

                log.InfoFormat("Successfully created and removed MySQL database: {0}", NewDatabase);
            }
            catch (Exception ex)
            {
                result.IsSuccess = false;
                result.EndUserMessage = ex.Message;

                log.ErrorFormat("Failed to create or remove MySQL database '{0}': {1}", NewDatabase, ex.Message);
                log.Error(ex.StackTrace);
            }

            return result;
        }

        private void DropDatabase(MySqlConnection connection)
        {
            var dropDatabaseCommand = connection.CreateCommand();
            dropDatabaseCommand.CommandText = string.Format(Queries.DropDatabase, NewDatabase);
            ExecuteCommand(dropDatabaseCommand);
        }

        private void DropUser(MySqlConnection connection)
        {
            var dropUserCommand = connection.CreateCommand();
            dropUserCommand.CommandText = string.Format(Queries.DropUser, NewUserId);
            ExecuteCommand(dropUserCommand);
        }

        private void GrantPrivileges(MySqlConnection connection)
        {
            var grantPrivilegesCommand = connection.CreateCommand();
            grantPrivilegesCommand.CommandText = string.Format(Queries.GrantAllPrivilegesToDatabase, NewDatabase, NewUserId);
            ExecuteCommand(grantPrivilegesCommand);
        }

        private void CreateDatabase(MySqlConnection connection)
        {
            var createDatabaseCommand = connection.CreateCommand();
            createDatabaseCommand.CommandText = string.Format(Queries.CreateDatabase, NewDatabase, NewUserId);
            ExecuteCommand(createDatabaseCommand);
        }

        private void CreateUser(MySqlConnection connection)
        {
            var createUserCommand = connection.CreateCommand();
            createUserCommand.CommandText = string.Format(Queries.CreateUser, NewUserId, NewPassword);
            ExecuteCommand(createUserCommand);
        }

        private static void ExecuteCommand(MySqlCommand command)
        {
            command.CommandType = CommandType.Text;
            command.ExecuteNonQuery();
        }

        private static string GetDatabaseName(AddonManifest manifest)
        {
            var developmentTeamAlias = manifest.CallingDeveloperAlias;
            var instanceAlias = manifest.InstanceAlias;

            return string.Format(DatabaseNameFormatter, developmentTeamAlias, instanceAlias);
        }

        private static string GetNewUsername(AddonManifest manifest)
        {
            var developmentTeamAlias = manifest.CallingDeveloperAlias;
            var instanceAlias = manifest.InstanceAlias;

            return string.Format(DatabaseUsernameFormatter, developmentTeamAlias, instanceAlias);
        }

        private static string GetPassword()
        {
            var guid = Guid.NewGuid();
            var now = DateTime.Now;
            var inputString = now.ToLongTimeString() + "__" + guid.ToString();
            
            byte[] bytes = Encoding.Unicode.GetBytes(inputString);
            SHA256Managed sha256 = new SHA256Managed();
            byte[] hash = sha256.ComputeHash(bytes);
            string password = string.Empty;
            foreach (byte x in hash)
            {
                password += String.Format("{0:x2}", x);
            }
            return password;
        }

        private MySqlConnection GetConnection(List<AddonProperty> properties)
        {
            ParseProperties(properties);

            return GetConnection(AdminConnectionString);
        }

        private MySqlConnection GetConnection(string connectionString)
        {
            var connection = new MySqlConnection(connectionString);
            connection.Open();
            return connection;
        }

        private void ParseProperties(List<AddonProperty> properties)
        {
            try
            {
                Server = properties.Find(p => p.Key == Keys.Server).Value;
                Port = int.Parse(properties.Find(p => p.Key == Keys.Port).Value);
                AdminUserId = properties.Find(p => p.Key == Keys.AdminUser).Value;
                AdminPassword = properties.Find(p => p.Key == Keys.AdminPassword).Value;
                AdminDatabase = properties.Find(p => p.Key == Keys.AdminDatabase).Value;
            }
            catch (Exception ex)
            {
                //TODO: Log failure...
            }
        }

    }
}
