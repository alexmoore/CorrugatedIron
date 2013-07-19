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

using CorrugatedIron.Comms;
using CorrugatedIron.Comms.LoadBalancing;
using CorrugatedIron.Config;
using CorrugatedIron.Extensions;
using CorrugatedIron.Messages;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CorrugatedIron
{
    public class RiakCluster : RiakEndPoint
    {
        private readonly RoundRobinStrategy _loadBalancer;
        private readonly List<IRiakNode> _nodes;
        private readonly ConcurrentQueue<IRiakNode> _offlineNodes;
        private readonly int _nodePollTime;
        private readonly int _defaultRetryCount;
        private bool _disposing;

        protected override int DefaultRetryCount
        {
            get { return _defaultRetryCount; }
        }

        public RiakCluster(IRiakClusterConfiguration clusterConfiguration, IRiakConnectionFactory connectionFactory)
        {
            _nodePollTime = clusterConfiguration.NodePollTime;
            _nodes = clusterConfiguration.RiakNodes.Select(rn => new RiakNode(rn, connectionFactory)).Cast<IRiakNode>().ToList();
            _loadBalancer = new RoundRobinStrategy();
            _loadBalancer.Initialise(_nodes);
            _offlineNodes = new ConcurrentQueue<IRiakNode>();
            _defaultRetryCount = clusterConfiguration.DefaultRetryCount;
            RetryWaitTime = clusterConfiguration.DefaultRetryWaitTime;

            Task.Factory.StartNew(NodeMonitor);
        }

        /// <summary>
        /// Creates an instance of <see cref="IRiakFooClient"/> populated from from the configuration section
        /// specified by <paramref name="configSectionName"/>.
        /// </summary>
        /// <param name="configSectionName">The name of the configuration section to load the settings from.</param>
        /// <returns>A fully configured <see cref="IRiakEndPoint"/></returns>
        public static IRiakEndPoint FromConfig(string configSectionName)
        {
            return new RiakCluster(RiakClusterConfiguration.LoadFromConfig(configSectionName), new RiakConnectionFactory());
        }

        /// <summary>
        /// Creates an instance of <see cref="IRiakFooClient"/> populated from from the configuration section
        /// specified by <paramref name="configSectionName"/>.
        /// </summary>
        /// <param name="configSectionName">The name of the configuration section to load the settings from.</param>
        /// <param name="configFileName">The full path and name of the config file to load the configuration from.</param>
        /// <returns>A fully configured <see cref="IRiakEndPoint"/></returns>
        public static IRiakEndPoint FromConfig(string configSectionName, string configFileName)
        {
            return new RiakCluster(RiakClusterConfiguration.LoadFromConfig(configSectionName, configFileName), new RiakConnectionFactory());
        }

        public override Task<RiakResult> UseConnection(Func<IRiakConnection, Task<RiakResult>> useFun, int retryAttempts)
        {
            if(retryAttempts < 0) return RiakResult.Error(ResultCode.NoRetries, "Unable to access a connection on the cluster.", false).ToTask();
            if (_disposing) return RiakResult.Error(ResultCode.ShuttingDown, "System currently shutting down", true).ToTask();

            var node = _loadBalancer.SelectNode();

            if (node != null)
            {
                return node.UseConnection(useFun)
                    .ContinueWith(t =>
                        {
                            if (!t.Result.IsSuccess)
                            {
                                RiakResult nextResult = null;
                                if (t.Result.ResultCode == ResultCode.NoConnections)
                                {
                                    Thread.Sleep(RetryWaitTime);
                                    nextResult = UseConnection(useFun, retryAttempts - 1).Result;
                                }
                                else if (t.Result.ResultCode == ResultCode.CommunicationError)
                                {
                                    if (t.Result.NodeOffline)
                                    {
                                        DeactivateNode(node);
                                    }

                                    Thread.Sleep(RetryWaitTime);
                                    nextResult = UseConnection(useFun, retryAttempts - 1).Result;
                                }

                                // if the next result is successful then return that
                                if (nextResult != null && nextResult.IsSuccess)
                                {
                                    return nextResult;
                                }

                                // otherwise we'll return the result that we had at this call to make sure that
                                // the correct/initial error is shown
                                return RiakResult.Error(t.Result.ResultCode, t.Result.ErrorMessage, t.Result.NodeOffline);
                            }
                            return t.Result;
                        });
            }
            return RiakResult.Error(ResultCode.ClusterOffline, "Unable to access functioning Riak node", true).ToTask();
        }

        public override Task<RiakResult<TResult>> UseConnection<TResult>(Func<IRiakConnection, Task<RiakResult<TResult>>> useFun, int retryAttempts)
        {
            if(retryAttempts < 0) return RiakResult<TResult>.Error(ResultCode.NoRetries, "Unable to access a connection on the cluster.", false).ToTask();
            if (_disposing) return RiakResult<TResult>.Error(ResultCode.ShuttingDown, "System currently shutting down", true).ToTask();

            var node = _loadBalancer.SelectNode();

            if (node != null)
            {
                return node.UseConnection(useFun)
                    .ContinueWith(t =>
                        {
                            if (!t.Result.IsSuccess)
                            {
                                RiakResult<TResult> nextResult = null;
                                if (t.Result.ResultCode == ResultCode.NoConnections)
                                {
                                    Thread.Sleep(RetryWaitTime);
                                    nextResult = UseConnection(useFun, retryAttempts - 1).Result;
                                }
                                else if (t.Result.ResultCode == ResultCode.CommunicationError)
                                {
                                    if (t.Result.NodeOffline)
                                    {
                                        DeactivateNode(node);
                                    }

                                    Thread.Sleep(RetryWaitTime);
                                    nextResult = UseConnection(useFun, retryAttempts - 1).Result;
                                }

                                // if the next result is successful then return that
                                if (nextResult != null && nextResult.IsSuccess)
                                {
                                    return nextResult;
                                }

                                // otherwise we'll return the result that we had at this call to make sure that
                                // the correct/initial error is shown
                                return RiakResult<TResult>.Error(t.Result.ResultCode, t.Result.ErrorMessage, t.Result.NodeOffline);
                            }
                            return t.Result;
                        });
            }
            return RiakResult<TResult>.Error(ResultCode.ClusterOffline, "Unable to access functioning Riak node", true).ToTask();
        }

        private void DeactivateNode(IRiakNode node)
        {
            lock (node)
            {
                if (!_offlineNodes.Contains(node))
                {
                    _loadBalancer.RemoveNode(node);
                    _offlineNodes.Enqueue(node);
                }
            }
        }

        private void NodeMonitor()
        {
            while (!_disposing)
            {
                var deadNodes = new List<IRiakNode>();
                IRiakNode node = null;
                while (_offlineNodes.TryDequeue(out node) && !_disposing)
                {
                    var result = node.UseConnection(c => c.PbcWriteRead(MessageCode.PingReq, MessageCode.PingResp)).Result;

                    if (result.IsSuccess)
                    {
                        _loadBalancer.AddNode(node);
                    }
                    else
                    {
                        deadNodes.Add(node);
                    }
                }

                if (!_disposing)
                {
                    foreach (var deadNode in deadNodes)
                    {
                        _offlineNodes.Enqueue(deadNode);
                    }

                    Thread.Sleep(_nodePollTime);
                }
            }
        }

        public override void Dispose()
        {
            _disposing = true;

            _nodes.ForEach(n => n.Dispose());
        }
    }
}
