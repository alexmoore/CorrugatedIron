// Copyright (c) 2011 - OJ Reeves & Jeremiah Peschka
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System.Threading.Tasks;
using CorrugatedIron.Comms;
using CorrugatedIron.Messages;
using CorrugatedIron.Models;
using CorrugatedIron.Models.Rest;
using CorrugatedIron.Util;
using Moq;
using NUnit.Framework;
using System;
using System.Linq;

namespace CorrugatedIron.Tests.RiakClientSetBucketPropertiesTests
{
    public class MockCluster : IRiakEndPoint
    {
        public Mock<IRiakConnection> ConnectionMock = new Mock<IRiakConnection>();

        public MockCluster()
        {
            RetryWaitTime = 200;
        }

        public void Dispose()
        {
        }

        public IRiakClient CreateClient()
        {
            return new Mock<IRiakClient>().Object;
        }

        public IRiakAsyncClient CreateAsyncClient()
        {
            return new Mock<IRiakAsyncClient>().Object;
        }

        public Task<RiakResult<TResult>> UseConnection<TResult>(Func<IRiakConnection, Task<RiakResult<TResult>>> useFun, int retryAttempts)
        {
            return useFun(ConnectionMock.Object);
        }

        public Task<RiakResult> UseConnection(Func<IRiakConnection, Task<RiakResult>> useFun, int retryAttempts)
        {
            return useFun(ConnectionMock.Object);
        }

        public int RetryWaitTime { get; set; }
    }

    public abstract class RiakClientSetBucketPropertiesTestBase
    {
        protected MockCluster Cluster;
        protected IRiakAsyncClient AsyncClient;
        protected IRiakClient Client;
        protected byte[] ClientId;

        protected Task<TResult> ToResult<TResult>(TResult result)
        {
            var tcs = new TaskCompletionSource<TResult>();
            tcs.SetResult(result);
            return tcs.Task;
        }

        protected RiakClientSetBucketPropertiesTestBase()
        {
            Cluster = new MockCluster();
            ClientId = System.Text.Encoding.Default.GetBytes("fadjskl").Take(4).ToArray();
            AsyncClient = new RiakAsyncClient(Cluster);
            Client = new RiakClient(AsyncClient);
        }
    }

    [TestFixture]
    public class WhenSettingBucketPropertiesWithExtendedProperties : RiakClientSetBucketPropertiesTestBase
    {
        protected RiakResult Response;
        [SetUp]
        public void SetUp()
        {
            var result = RiakResult<RiakRestResponse>.Success(new RiakRestResponse { StatusCode = System.Net.HttpStatusCode.NoContent });
            Cluster.ConnectionMock.Setup(m => m.RestRequest(It.IsAny<RiakRestRequest>())).Returns(ToResult(result));

            Response = Client.SetBucketProperties("foo", new RiakBucketProperties().SetAllowMultiple(true).SetRVal("one"));
        }

        [Test]
        [Ignore]
        public void RestInterfaceIsInvokedWithAppropriateValues()
        {
            Cluster.ConnectionMock.Verify(m => m.RestRequest(It.Is<RiakRestRequest>(r => r.ContentType == RiakConstants.ContentTypes.ApplicationJson
                && r.Method == RiakConstants.Rest.HttpMethod.Put)), Times.Once());
        }
    }

    [TestFixture]
    public class WhenSettingBucketPropertiesWithoutExtendedProperties : RiakClientSetBucketPropertiesTestBase
    {
        protected RiakResult Response;
        [SetUp]
        public void SetUp()
        {
            var result = RiakResult.Success();
            Cluster.ConnectionMock.Setup(m => m.PbcWriteRead(It.IsAny<RpbSetBucketReq>(), MessageCode.SetBucketResp)).Returns(ToResult(result));

            Response = Client.SetBucketProperties("foo", new RiakBucketProperties().SetAllowMultiple(true), true);
        }

        [Test]
        public void PbcInterfaceIsInvokedWithAppropriateValues()
        {
            Cluster.ConnectionMock.Verify(m => m.PbcWriteRead(It.Is<RpbSetBucketReq>(r => r.props.allow_mult), MessageCode.SetBucketResp), Times.Once());
        }
    }
}

