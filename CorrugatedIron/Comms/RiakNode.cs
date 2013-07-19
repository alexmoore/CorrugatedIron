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
using CorrugatedIron.Config;
using System;
using CorrugatedIron.Extensions;

namespace CorrugatedIron.Comms
{
    public interface IRiakNode : IDisposable
    {
        Task<RiakResult<TResult>> UseConnection<TResult>(Func<IRiakConnection, Task<RiakResult<TResult>>> useFun);
        Task<RiakResult> UseConnection(Func<IRiakConnection, Task<RiakResult>> useFun);
    }

    public class RiakNode : IRiakNode
    {
        private readonly IRiakConnectionManager _connections;
        private bool _disposing;

        public RiakNode(IRiakNodeConfiguration nodeConfiguration, IRiakConnectionFactory connectionFactory)
        {
            // assume that if the node has a pool size of 0 then the intent is to have the connections
            // made on the fly
            if (nodeConfiguration.PoolSize == 0)
            {
                _connections = new RiakOnTheFlyConnection(nodeConfiguration, connectionFactory);
            }
            else
            {
                _connections = new RiakConnectionPool(nodeConfiguration, connectionFactory);
            }
        }

        public Task<RiakResult> UseConnection(Func<IRiakConnection, Task<RiakResult>> useFun)
        {
            return UseConnection(useFun, RiakResult.Error);
        }

        public Task<RiakResult<TResult>> UseConnection<TResult>(Func<IRiakConnection, Task<RiakResult<TResult>>> useFun)
        {
            return UseConnection(useFun, RiakResult<TResult>.Error);
        }

        private Task<TRiakResult> UseConnection<TRiakResult>(Func<IRiakConnection, Task<TRiakResult>> useFun, Func<ResultCode, string, bool, TRiakResult> onError)
            where TRiakResult : RiakResult
        {
            if(_disposing) return onError(ResultCode.ShuttingDown, "Connection is shutting down", true).ToTask();

            return _connections.Consume(useFun)
                .ContinueWith(t =>
                    {
                        if (t.Result.Item1)
                        {
                            return t.Result.Item2;
                        }
                        return onError(ResultCode.NoConnections, "Unable to acquire connection", true);
                    });
        }

        public void Dispose()
        {
            _disposing = true;
            _connections.Dispose();
        }
    }
}