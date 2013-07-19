// Copyright (c) 2013 - OJ Reeves & Jeremiah Peschka
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
    internal class RiakOnTheFlyConnection : IRiakConnectionManager
    {
        private readonly IRiakNodeConfiguration _nodeConfig;
        private readonly IRiakConnectionFactory _connFactory;
        private bool _disposing;

        public RiakOnTheFlyConnection(IRiakNodeConfiguration nodeConfig, IRiakConnectionFactory connFactory)
        {
            _nodeConfig = nodeConfig;
            _connFactory = connFactory;
        }

        public Task<Tuple<bool, TResult>> Consume<TResult>(Func<IRiakConnection, Task<TResult>> consumer)
        {
            if(_disposing) return Tuple.Create(false, default(TResult)).ToTask();

            var conn = _connFactory.CreateConnection(_nodeConfig);
            return consumer(conn)
                .ContinueWith(t =>
                    {
                        if (conn != null) conn.Dispose();
                        return t.IsFaulted
                            ? Tuple.Create(false, default(TResult))
                            : Tuple.Create(true, t.Result);
                    });
        }

        public void Dispose()
        {
            if(_disposing) return;

            _disposing = true;
        }
    }
}
