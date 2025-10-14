using Xunit;
using Moq;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Collections.Generic;
using AstraDb.Driver.Abstractions;
using AstraDb.Driver.Decorators;

namespace AstraDb.Driver.Tests
{
    public class LoggingAstraDbClientDecoratorTests
    {
        [Fact]
        public async Task Should_Call_Inner_Client_On_ReadAsync()
        {
            var innerMock = new Mock<IAstraDbClient>();
            innerMock.Setup(x => x.ReadAsync<object>("ks","tbl",It.IsAny<IDictionary<string,object>>()))
                .ReturnsAsync(new List<object>{new {}});

            var loggerMock = new Mock<ILogger<LoggingAstraDbClientDecorator>>();
            var decorator = new LoggingAstraDbClientDecorator(innerMock.Object, loggerMock.Object);

            var result = await decorator.ReadAsync<object>("ks","tbl", new Dictionary<string, object>{{"id",1}});

            Assert.NotNull(result);
            innerMock.Verify(x => x.ReadAsync<object>("ks","tbl",It.IsAny<IDictionary<string,object>>()), Times.Once);
        }
    }
}
