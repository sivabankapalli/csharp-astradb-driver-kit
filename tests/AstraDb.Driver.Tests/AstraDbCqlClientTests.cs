using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AstraDb.Driver.Abstractions;
using AstraDb.Driver.Config;
using AstraDb.Driver.Extensions;
using AstraDb.Driver.Implementations;
using AstraDb.Driver.Internal;
using Cassandra;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Xunit;

namespace AstraDb.Driver.Tests;

public class AstraDbCqlClientTests
{
    [Fact]
    public void AddAstraDbDriver_Should_Throw_When_Invalid_Config()
    {
        // Arrange
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();

        // Act
        Action act = () => services.AddAstraDbDriver(config.GetSection("Astra:Driver"));

        // Assert
        Assert.Throws<InvalidOperationException>(act);
    }

    [Fact]
    public void AddAstraDbDriver_Should_Register_Dependencies()
    {
        // Arrange
        var mockCluster = new Mock<ICluster>();
        var mockSession = new Mock<ISession>();
        var services = new ServiceCollection();
        services.AddSingleton(
            new AstraDbConnectionOptions
            {
                SecureConnectBundlePath = "secure-connect.zip",
                Keyspace = "demo",
                Token = "AstraCS:xxxxxx"
            });
        services.AddSingleton(mockCluster.Object);
        services.AddSingleton(mockSession.Object);
        services.AddSingleton<IAstraDbClient, AstraDbCqlClient>();

        // Act
        var provider = services.BuildServiceProvider();

        // Assert
        Assert.NotNull(provider.GetRequiredService<AstraDbConnectionOptions>());
        Assert.NotNull(provider.GetRequiredService<ICluster>());
        Assert.NotNull(provider.GetRequiredService<ISession>());
        Assert.NotNull(provider.GetRequiredService<IAstraDbClient>());
    }

    [Fact]
    public async Task AstraDbCqlClient_Should_Throw_NotImplemented()
    {
        // Arrange
        var mockCluster = new Mock<ICluster>();
        var mockSession = new Mock<ISession>();
        var client = new AstraDbCqlClient(mockCluster.Object, mockSession.Object);

        // Act
        Func<Task> act = () => client.ReadAsync<object>("ks", "tbl", new Dictionary<string, object> { { "id", 1 } });

        // Assert
        await Assert.ThrowsAsync<NotImplementedException>(act);
    }

    [Fact]
    public void Constructor_Should_Throw_On_Null_Arguments()
    {
        // Arrange
        var mockCluster = new Mock<ICluster>().Object;
        var mockSession = new Mock<ISession>().Object;

        // Act
        Action act1 = () => new AstraDbCqlClient(null!, mockSession);
        Action act2 = () => new AstraDbCqlClient(mockCluster, null!);

        // Assert
        Assert.Throws<ArgumentNullException>(act1);
        Assert.Throws<ArgumentNullException>(act2);
    }

    [Fact]
    public async Task DisposeAsync_Should_Shutdown_Cluster()
    {
        // Arrange
        var clusterMock = new Mock<ICluster>();
        var sessionMock = new Mock<ISession>();

        clusterMock.Setup(c => c.ShutdownAsync(It.IsAny<int>()))
                   .Returns(Task.CompletedTask);

        var client = new AstraDbCqlClient(clusterMock.Object, sessionMock.Object);

        // Act
        await client.DisposeAsync();

        // Assert
        clusterMock.Verify(c => c.ShutdownAsync(It.IsAny<int>()), Times.Once);
    }

    [Fact]
    public async Task WriteAsync_Should_Insert_Data_Successfully()
    {
        // Arrange
        var (clusterMock, sessionMock, preparedMock, boundMock, client) = CreateClientWithPreparedSession();

        var fields = Fields(("id", 1), ("name", "Alice"));

        // Act
        var result = await client.WriteAsync("ks", "users", fields);

        // Assert
        Assert.True(result.Success);
        sessionMock.Verify(s => s.PrepareAsync(It.Is<string>(q => q.Contains("INSERT INTO"))), Times.Once);
        sessionMock.Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Once);
    }

    [Fact]
    public async Task WriteAsync_Should_Return_False_On_DriverException()
    {
        // Arrange
        var driverEx = new OperationTimedOutException(null, 0);
        var (clusterMock, sessionMock, preparedMock, boundMock, client) = CreateClientWithPreparedSession(executeException: driverEx);

        // Act
        var result = await client.WriteAsync("ks", "tbl_driver_ex", Fields(("id", 1)));

        // Assert
        Assert.False(result.Success);
    }

    [Fact]
    public async Task WriteAsync_Should_Return_False_On_Unexpected_Exception()
    {
        // Arrange
        var unexpected = new InvalidOperationException("boom");
        var (clusterMock, sessionMock, preparedMock, boundMock, client) = CreateClientWithPreparedSession(executeException: unexpected);

        // Act
        var result = await client.WriteAsync("ks", "tbl_unexpected_ex", Fields(("id", 1)));

        // Assert
        Assert.False(result.Success);
    }

    [Fact]
    public async Task WriteAsync_WithPoco_Uses_Mapper_Delegate()
    {
        // Arrange
        var (clusterMock, sessionMock, preparedMock, boundMock, client) = CreateClientWithPreparedSession();
        var poco = new { Id = 101, Name = "Siva" };
        Func<dynamic, IReadOnlyDictionary<string, object?>> mapper =
            p => new Dictionary<string, object?> { ["id"] = p.Id, ["name"] = p.Name };

        // Act
        var result = await client.WriteAsync("ks", "user", poco, mapper);

        // Assert
        Assert.True(result.Success);
        sessionMock.Verify(s => s.ExecuteAsync(It.IsAny<IStatement>()), Times.Once);
    }

    [Fact]
    public void KeyspaceResolver_Should_Resolve_Explicit_And_Session_Keyspace()
    {
        // Arrange
        var sessionMock = new Mock<ISession>();
        sessionMock.Setup(s => s.Keyspace).Returns("default_ks");

        // Act
        var result1 = KeyspaceResolver.Resolve(sessionMock.Object, "explicit_ks");
        var result2 = KeyspaceResolver.Resolve(sessionMock.Object, null);

        // Assert
        Assert.Equal("explicit_ks", result1);
        Assert.Equal("default_ks", result2);
    }

    [Fact]
    public void KeyspaceResolver_Should_Throw_If_No_Keyspace()
    {
        // Arrange
        var sessionMock = new Mock<ISession>();
        sessionMock.Setup(s => s.Keyspace).Returns((string)null!);

        // Act
        Action act = () => KeyspaceResolver.Resolve(sessionMock.Object, "");

        // Assert
        Assert.Throws<ArgumentException>(act);
    }

    [Fact]
    public void CqlInsertBuilder_Should_Build_Correct_CQL()
    {
        // Arrange
        var cols = new[] { "id", "name" };

        // Act
        var cql = CqlInsertBuilder.BuildInsert("ks", "users", cols, false, null, null);

        // Assert
        Assert.Contains("INSERT INTO ks.users (id, name)", cql);
        Assert.Contains("VALUES (?, ?)", cql);
    }

    [Fact]
    public void CqlInsertBuilder_Should_Build_Cache_Key()
    {
        // Arrange
        var cols = new[] { "id", "name" };

        // Act
        var key = CqlInsertBuilder.BuildCacheKey("KS", "Users", cols, true, 3600, DateTimeOffset.UtcNow);

        // Assert
        Assert.Contains("ifne", key, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ttl", key, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ts", key, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void CqlInsertBuilder_Should_Quote_Identifiers_Correctly()
    {
        // Act & Assert
        Assert.Equal("\"User-Table\"", CqlInsertBuilder.QuoteId("User-Table"));
        Assert.Equal("valid_id", CqlInsertBuilder.QuoteId("valid_id"));
    }

    [Fact]
    public async Task PreparedStatementCache_Should_Prepare_Once_And_ReUse()
    {
        // Arrange
        var sessionMock = new Mock<ISession>();
        var prepared = new Mock<PreparedStatement>().Object;

        sessionMock.Setup(s => s.PrepareAsync(It.IsAny<string>())).ReturnsAsync(prepared);

        var cache = new PreparedStatementCache();

        // Act
        var ps1 = await cache.GetOrPrepareAsync(sessionMock.Object, "CQL", "key");
        var ps2 = await cache.GetOrPrepareAsync(sessionMock.Object, "CQL", "key");

        // Assert
        Assert.Same(ps1, ps2);
        sessionMock.Verify(s => s.PrepareAsync("CQL"), Times.Once);
    }

    // --- Helpers to reduce duplication ------------------------------------------------

    private static (Mock<ICluster> clusterMock, Mock<ISession> sessionMock, Mock<PreparedStatement> preparedMock, Mock<BoundStatement> boundMock, AstraDbCqlClient client)
        CreateClientWithPreparedSession(RowSet? executeResult = null, Exception? executeException = null, string keyspace = "ks")
    {
        var clusterMock = new Mock<ICluster>();
        var sessionMock = new Mock<ISession>();
        var preparedMock = new Mock<PreparedStatement>();
        var boundMock = new Mock<BoundStatement>();

        sessionMock.Setup(s => s.Keyspace).Returns(keyspace);
        sessionMock.Setup(s => s.PrepareAsync(It.IsAny<string>())).ReturnsAsync(preparedMock.Object);
        preparedMock.Setup(p => p.Bind(It.IsAny<object[]>())).Returns(boundMock.Object);

        if (executeException is not null)
        {
            sessionMock.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                       .ThrowsAsync(executeException);
        }
        else
        {
            sessionMock.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
                       .ReturnsAsync(executeResult);
        }

        var client = new AstraDbCqlClient(clusterMock.Object, sessionMock.Object);
        return (clusterMock, sessionMock, preparedMock, boundMock, client);
    }

    private static Dictionary<string, object?> Fields(params (string name, object? value)[] pairs)
        => pairs.ToDictionary(p => p.name, p => p.value);
}