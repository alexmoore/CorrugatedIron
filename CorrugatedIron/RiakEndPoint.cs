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

using System.Threading;
using System.Threading.Tasks;
using CorrugatedIron.Comms;
using System;

namespace CorrugatedIron
{
    public abstract class RiakEndPoint : IRiakEndPoint
    {
        private readonly AutoResetEvent _waitHandle;

        protected abstract int DefaultRetryCount { get; }

        public int RetryWaitTime { get; set; }

        protected RiakEndPoint()
        {
            _waitHandle = new AutoResetEvent(false);
        }

        /// <summary>
        /// Creates a new instance of <see cref="RiakAsyncClient"/>.
        /// </summary>
        /// <returns>
        /// A minty fresh client.
        /// </returns>
        public IRiakAsyncClient CreateAsyncClient()
        {
            return new RiakAsyncClient(this) { RetryCount = DefaultRetryCount };
        }

        /// <summary>
        /// Creates a new instance of <see cref="RiakClient"/>.
        /// </summary>
        /// <returns>
        /// A minty fresh client.
        /// </returns>
        public IRiakClient CreateClient()
        {
            return new RiakClient(CreateAsyncClient());
        }

        public abstract Task<RiakResult> UseConnection(Func<IRiakConnection, Task<RiakResult>> useFun, int retryAttempts);

        public abstract Task<RiakResult<TResult>> UseConnection<TResult>(Func<IRiakConnection, Task<RiakResult<TResult>>> useFun, int retryAttempts);

        public abstract void Dispose();

        protected void SafeWait(int ms)
        {
            _waitHandle.WaitOne(ms);
        }
    }
}