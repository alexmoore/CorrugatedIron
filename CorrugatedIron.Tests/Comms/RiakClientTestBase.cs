﻿// Copyright (c) 2010 - OJ Reeves & Jeremiah Peschka
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

using System.Collections.Generic;
using CorrugatedIron.Comms;
using CorrugatedIron.Config;
using Moq;

namespace CorrugatedIron.Tests.Comms
{
    public abstract class RiakClientTestBase<TRequest, TResult>
        where TResult : new()
    {
        protected RiakResult<TResult> Result;
        protected Mock<IRiakConnection> ConnMock;
        protected Mock<IRiakNodeConfiguration> NodeConfigMock;
        protected Mock<IRiakClusterConfiguration> ClusterConfigMock;
        protected Mock<IRiakConnectionFactory> ConnFactoryMock;
        protected RiakCluster Cluster;
        protected RiakClient Client;

        protected void SetUpInternal()
        {
            ConnMock = new Mock<IRiakConnection>();
            ClusterConfigMock = new Mock<IRiakClusterConfiguration>();
            ConnFactoryMock = new Mock<IRiakConnectionFactory>();
            NodeConfigMock = new Mock<IRiakNodeConfiguration>();

            ConnMock.Setup(m => m.PbcWriteRead<TRequest, TResult>(It.IsAny<TRequest>())).Returns(() => Result);
            ConnMock.Setup(m => m.SetClientId(It.IsAny<byte[]>())).Returns(RiakResult.Success());
            ConnFactoryMock.Setup(m => m.CreateConnection(It.IsAny<IRiakNodeConfiguration>())).Returns(ConnMock.Object);
            NodeConfigMock.SetupGet(m => m.PoolSize).Returns(1);
            NodeConfigMock.SetupGet(m => m.Name).Returns("dev1");
            ClusterConfigMock.SetupGet(m => m.RiakNodes).Returns(new List<IRiakNodeConfiguration> { NodeConfigMock.Object });

            Cluster = new RiakCluster(ClusterConfigMock.Object, new RiakNodeFactory(), ConnFactoryMock.Object);
            Client = new RiakClient(Cluster);
        }
    }
}
