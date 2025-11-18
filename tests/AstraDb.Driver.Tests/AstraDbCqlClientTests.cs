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
using Cassandra.Mapping;
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
        var mockMapper = new Mock<IMapper>();

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
        services.AddSingleton(mockMapper.Object);
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
        var mockMapper = new Mock<IMapper>();
        var client = new AstraDbCqlClient(mockCluster.Object, mockSession.Object, mockMapper.Object);

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
        var mockMapper = new Mock<IMapper>().Object;

        // Act
        Action act1 = () => new AstraDbCqlClient(null!, mockSession, mockMapper);
        Action act2 = () => new AstraDbCqlClient(mockCluster, null!, mockMapper);

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
        var mockMapper = new Mock<IMapper>();

        clusterMock.Setup(c => c.ShutdownAsync(It.IsAny<int>()))
                   .Returns(Task.CompletedTask);

        var client = new AstraDbCqlClient(clusterMock.Object, sessionMock.Object, mockMapper.Object);

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

    [Fact]
    public void Constructor_Should_Throw_On_Null_Mapper() // NEW TEST
    {
        // Arrange
        var mockCluster = new Mock<ICluster>().Object;
        var mockSession = new Mock<ISession>().Object;

        // Act
        Action act = () => new AstraDbCqlClient(mockCluster, mockSession, null!);

        // Assert
        Assert.Throws<ArgumentNullException>(act);
    }

    [Fact]
    public async Task WriteAsync_Should_Throw_When_Keyspace_Invalid() // NEW TEST
    {
        // Arrange
        var client = new AstraDbCqlClient(
            new Mock<ICluster>().Object,
            new Mock<ISession>().Object,
            new Mock<IMapper>().Object);

        var fields = new Dictionary<string, object?> { { "id", 1 } };

        // Act
        Func<Task> act = () => client.WriteAsync("", "tbl", fields);

        // Assert
        var ex = await Assert.ThrowsAsync<ArgumentException>(act);
        Assert.Contains("keyspace required", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task WriteAsync_Should_Throw_When_Table_Invalid() // NEW TEST
    {
        // Arrange
        var client = new AstraDbCqlClient(
            new Mock<ICluster>().Object,
            new Mock<ISession>().Object,
            new Mock<IMapper>().Object);

        var fields = new Dictionary<string, object?> { { "id", 1 } };

        // Act
        Func<Task> act = () => client.WriteAsync("ks", " ", fields);

        // Assert
        var ex = await Assert.ThrowsAsync<ArgumentException>(act);
        Assert.Contains("table required", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task WriteAsync_Should_Throw_When_Fields_Null() // NEW TEST
    {
        // Arrange
        var client = new AstraDbCqlClient(
            new Mock<ICluster>().Object,
            new Mock<ISession>().Object,
            new Mock<IMapper>().Object);

        // Act
        Func<Task> act = () => client.WriteAsync("ks", "tbl", null!);

        // Assert
        var ex = await Assert.ThrowsAsync<ArgumentException>(act);
        Assert.Contains("At least one column/value is required", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task WriteAsync_Should_Throw_When_Fields_Empty() // NEW TEST
    {
        // Arrange
        var client = new AstraDbCqlClient(
            new Mock<ICluster>().Object,
            new Mock<ISession>().Object,
            new Mock<IMapper>().Object);

        var fields = new Dictionary<string, object?>();

        // Act
        Func<Task> act = () => client.WriteAsync("ks", "tbl", fields);

        // Assert
        var ex = await Assert.ThrowsAsync<ArgumentException>(act);
        Assert.Contains("At least one column/value is required", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task WriteAsync_WithPoco_Should_Throw_When_Document_Null() // NEW TEST
    {
        // Arrange
        var client = new AstraDbCqlClient(
            new Mock<ICluster>().Object,
            new Mock<ISession>().Object,
            new Mock<IMapper>().Object);

        // Act
        Func<Task> act = () => client.WriteAsync<object>("ks", "tbl", null!, _ => new Dictionary<string, object?>());

        // Assert
        await Assert.ThrowsAsync<ArgumentNullException>(act);
    }

    [Fact]
    public async Task WriteAsync_WithPoco_Should_Throw_When_Mapper_Null() // NEW TEST
    {
        // Arrange
        var client = new AstraDbCqlClient(
            new Mock<ICluster>().Object,
            new Mock<ISession>().Object,
            new Mock<IMapper>().Object);

        var poco = new { Id = 1 };

        // Act
        Func<Task> act = () => client.WriteAsync("ks", "tbl", poco, null!);

        // Assert
        await Assert.ThrowsAsync<ArgumentNullException>(act);
    }

    [Fact]
    public async Task WriteAsync_WithPoco_Should_Throw_When_Mapper_Returns_Null() // NEW TEST
    {
        // Arrange
        var client = new AstraDbCqlClient(
            new Mock<ICluster>().Object,
            new Mock<ISession>().Object,
            new Mock<IMapper>().Object);

        var poco = new { Id = 1 };

        // Act
        Func<Task> act = () => client.WriteAsync("ks", "tbl", poco, _ => null!);

        // Assert
        await Assert.ThrowsAsync<ArgumentException>(act);
    }

    // ---------- Generic ReadAsync<T>(filters) using _mapper ----------

    public class SampleEntity
    {
        public int Id { get; set; }
    }

    [Fact]
    public async Task ReadAsync_Generic_Should_Return_All_When_No_Filters()
    {
        // Arrange
        var cluster = new Mock<ICluster>();
        var session = new Mock<ISession>();
        var mapper = new Mock<IMapper>();

        var expected = new List<SampleEntity>
    {
        new SampleEntity { Id = 1 },
        new SampleEntity { Id = 2 }
    };

        // Match the overload used by your code: FetchAsync<T>(CqlQueryOptions options = null)
        mapper.Setup(m => m.FetchAsync<SampleEntity>(It.IsAny<CqlQueryOptions>()))
              .ReturnsAsync(expected);

        var client = new AstraDbCqlClient(cluster.Object, session.Object, mapper.Object);

        // Act
        var result = await client.ReadAsync<SampleEntity>();

        // Assert
        Assert.Same(expected, result);
        mapper.Verify(m => m.FetchAsync<SampleEntity>(It.IsAny<CqlQueryOptions>()), Times.Once);
    }

    [Fact]
    public async Task ReadAsync_Generic_Should_Use_Filters_When_Provided() // NEW TEST
    {
        // Arrange
        var cluster = new Mock<ICluster>();
        var session = new Mock<ISession>();
        var mapper = new Mock<IMapper>();

        var expected = new List<SampleEntity>
        {
            new SampleEntity { Id = 10 }
        };

        mapper.Setup(m => m.FetchAsync<SampleEntity>(It.IsAny<Cql>()))
              .ReturnsAsync(expected);

        var client = new AstraDbCqlClient(cluster.Object, session.Object, mapper.Object);
        var filters = new Dictionary<string, object> { ["id"] = 10 };

        // Act
        var result = await client.ReadAsync<SampleEntity>(filters);

        // Assert
        Assert.Same(expected, result);
        mapper.Verify(m => m.FetchAsync<SampleEntity>(It.IsAny<Cql>()), Times.Once);
    }

    [Fact]
    public async Task ReadAsync_Generic_Should_Return_Empty_On_DriverException() // NEW TEST
    {
        // Arrange
        var cluster = new Mock<ICluster>();
        var session = new Mock<ISession>();
        var mapper = new Mock<IMapper>();

        mapper.Setup(m => m.FetchAsync<SampleEntity>(It.IsAny<Cql>()))
              .ThrowsAsync(new OperationTimedOutException(null, 0));

        var client = new AstraDbCqlClient(cluster.Object, session.Object, mapper.Object);

        // Act
        var result = await client.ReadAsync<SampleEntity>();

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public async Task ReadAsync_Generic_Should_Return_Empty_On_UnexpectedException() // NEW TEST
    {
        // Arrange
        var cluster = new Mock<ICluster>();
        var session = new Mock<ISession>();
        var mapper = new Mock<IMapper>();

        mapper.Setup(m => m.FetchAsync<SampleEntity>(It.IsAny<Cql>()))
              .ThrowsAsync(new InvalidOperationException("boom"));

        var client = new AstraDbCqlClient(cluster.Object, session.Object, mapper.Object);

        // Act
        var result = await client.ReadAsync<SampleEntity>();

        // Assert
        Assert.Empty(result);
    }

    // ---------- WriteAsync<T>(document) using _mapper ----------

    [Fact]
    public async Task WriteAsync_Generic_Should_Return_Success_On_Insert() // NEW TEST
    {
        // Arrange
        var cluster = new Mock<ICluster>();
        var session = new Mock<ISession>();
        var mapper = new Mock<IMapper>();

        mapper.Setup(m => m.InsertAsync(
                          It.IsAny<SampleEntity>(),
                          It.IsAny<CqlQueryOptions>()))
              .Returns(Task.CompletedTask);

        var client = new AstraDbCqlClient(cluster.Object, session.Object, mapper.Object);
        var entity = new SampleEntity { Id = 123 };

        // Act
        var result = await client.WriteAsync(entity);

        // Assert
        Assert.True(result.Success);
        mapper.Verify(m => m.InsertAsync(
                  It.Is<SampleEntity>(e => e == entity),
                  It.IsAny<CqlQueryOptions>()),
              Times.Once);
    }

    [Fact]
    public async Task WriteAsync_Generic_Should_Return_False_On_DriverException() // NEW TEST
    {
        // Arrange
        var cluster = new Mock<ICluster>();
        var session = new Mock<ISession>();
        var mapper = new Mock<IMapper>();

        mapper.Setup(m => m.InsertAsync(
                          It.IsAny<SampleEntity>(),
                          It.IsAny<CqlQueryOptions>()))
              .ThrowsAsync(new InvalidOperationException("boom"));


        var client = new AstraDbCqlClient(cluster.Object, session.Object, mapper.Object);
        var entity = new SampleEntity { Id = 123 };

        // Act
        var result = await client.WriteAsync(entity);

        // Assert
        Assert.False(result.Success);
    }

    [Fact]
    public async Task WriteAsync_Generic_Should_Return_False_On_UnexpectedException() // NEW TEST
    {
        // Arrange
        var cluster = new Mock<ICluster>();
        var session = new Mock<ISession>();
        var mapper = new Mock<IMapper>();

        mapper.Setup(m => m.InsertAsync(
                          It.IsAny<SampleEntity>(),
                          It.IsAny<CqlQueryOptions>()))
              .ThrowsAsync(new InvalidOperationException("boom"));


        var client = new AstraDbCqlClient(cluster.Object, session.Object, mapper.Object);
        var entity = new SampleEntity { Id = 123 };

        // Act
        var result = await client.WriteAsync(entity);

        // Assert
        Assert.False(result.Success);
    }

    // ---------- DisposeAsync branch: session IDisposable ----------

    [Fact]
    public async Task DisposeAsync_Should_Dispose_Session_If_Disposable() // NEW TEST
    {
        // Arrange
        var clusterMock = new Mock<ICluster>();
        clusterMock.Setup(c => c.ShutdownAsync(It.IsAny<int>()))
                   .Returns(Task.CompletedTask);

        var sessionMock = new Mock<ISession>();
        var sessionDisposable = sessionMock.As<IDisposable>();
        sessionDisposable.Setup(d => d.Dispose());

        var mapper = new Mock<IMapper>();

        var client = new AstraDbCqlClient(clusterMock.Object, sessionMock.Object, mapper.Object);

        // Act
        await client.DisposeAsync();

        // Assert
        sessionDisposable.Verify(d => d.Dispose(), Times.Once);
        clusterMock.Verify(c => c.ShutdownAsync(It.IsAny<int>()), Times.Once);
    }

    // ---------- PreparedStatementCache extra branch ----------

    [Fact]
    public async Task PreparedStatementCache_Should_Prepare_Separate_Keys_Independently() // NEW TEST
    {
        // Arrange
        var sessionMock = new Mock<ISession>();
        var prepared1 = new Mock<PreparedStatement>().Object;
        var prepared2 = new Mock<PreparedStatement>().Object;

        sessionMock.SetupSequence(s => s.PrepareAsync(It.IsAny<string>()))
                   .ReturnsAsync(prepared1)
                   .ReturnsAsync(prepared2);

        var cache = new PreparedStatementCache();

        // Act
        var ps1 = await cache.GetOrPrepareAsync(sessionMock.Object, "CQL1", "key1");
        var ps2 = await cache.GetOrPrepareAsync(sessionMock.Object, "CQL2", "key2");

        // Assert
        Assert.Same(prepared1, ps1);
        Assert.Same(prepared2, ps2);
        sessionMock.Verify(s => s.PrepareAsync("CQL1"), Times.Once);
        sessionMock.Verify(s => s.PrepareAsync("CQL2"), Times.Once);
    }

    // --- Helpers to reduce duplication ------------------------------------------------

    private static (Mock<ICluster> clusterMock, Mock<ISession> sessionMock, Mock<PreparedStatement> preparedMock, Mock<BoundStatement> boundMock, AstraDbCqlClient client)
        CreateClientWithPreparedSession(RowSet? executeResult = null, Exception? executeException = null, string keyspace = "ks")
    {
        var clusterMock = new Mock<ICluster>();
        var sessionMock = new Mock<ISession>();
        var preparedMock = new Mock<PreparedStatement>();
        var boundMock = new Mock<BoundStatement>();
        var mockMapper = new Mock<IMapper>();

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

        var client = new AstraDbCqlClient(clusterMock.Object, sessionMock.Object, mockMapper.Object);
        return (clusterMock, sessionMock, preparedMock, boundMock, client);
    }

    private static Dictionary<string, object?> Fields(params (string name, object? value)[] pairs)
        => pairs.ToDictionary(p => p.name, p => p.value);
}