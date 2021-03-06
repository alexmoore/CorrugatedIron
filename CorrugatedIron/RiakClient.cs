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

using System.Net.NetworkInformation;
using CorrugatedIron.Comms;
using CorrugatedIron.Extensions;
using CorrugatedIron.Messages;
using CorrugatedIron.Models;
using CorrugatedIron.Models.Index;
using CorrugatedIron.Models.MapReduce;
using CorrugatedIron.Models.MapReduce.Inputs;
using CorrugatedIron.Models.RiakDt;
using CorrugatedIron.Models.Search;
using CorrugatedIron.Models.Rest;
using CorrugatedIron.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Numerics;
using System.Net;

namespace CorrugatedIron
{
    public interface IRiakClient : IRiakBatchClient
    {
        void Batch(Action<IRiakBatchClient> batchAction);

        T Batch<T>(Func<IRiakBatchClient, T> batchFunction);

        IRiakAsyncClient Async { get; }
    }

    public class RiakClient : IRiakClient
    {
        private const string ListKeysWarning = "*** [CI] -> ListKeys is an expensive operation and should not be used in Production scenarios. ***";
        private const string InvalidBucketErrorMessage = "Bucket cannot be blank or contain forward-slashes";
        private const string InvalidKeyErrorMessage = "Key cannot be blank or contain forward-slashes";
        private const string InvalidBucketTypeErrorMessage = "Bucket type cannot be blank or contain forward-slashes";

        private readonly IRiakEndPoint _endPoint;
        private readonly IRiakConnection _batchConnection;

        public int RetryCount { get; set; }

        public IRiakAsyncClient Async { get; private set; }

        internal RiakClient(IRiakEndPoint endPoint)
        {
            _endPoint = endPoint;
            Async = new RiakAsyncClient(this);
        }

        private RiakClient(IRiakConnection batchConnection)
        {
            _batchConnection = batchConnection;
            Async = new RiakAsyncClient(this);
        }

        /// <summary>
        /// Ping this instance of Riak
        /// </summary>
        /// <description>Ping can be used to ensure that there is an operational Riak node
        /// present at the other end of the client. It's important to note that this will ping
        /// any Riak node in the cluster and a specific node cannot be specified by the user.
        /// Do not use this method to determine individual node health.</description>
        /// <returns>Returns true if the Riak instance has returned a 'pong' response. 
        /// Returns false if Riak is unavailable or returns a 'pang' response. </returns>
        public RiakResult Ping()
        {
            return UseConnection(conn => conn.PbcWriteRead(MessageCode.PingReq, MessageCode.PingResp));
        }

        /// <summary>
        /// Increments a Riak counter. 
        /// </summary>
        /// <param name="bucket">The bucket</param>
        /// <param name="counter">The name of the counter</param>
        /// <param name="amount">The amount to increment/decrement the counter</param>
        /// <param name="options">The <see cref="RiakCounterUpdateOptions"/></param>
        /// <returns><see cref="RiakCounterResult"/></returns>
        /// <remarks>Only available in Riak 1.4+. If the counter is not initialized, then the counter will be initialized to 0 and then incremented.</remarks>
        public RiakCounterResult IncrementCounter(string bucket, string counter, long amount, RiakCounterUpdateOptions options = null)
        {
            var request = new RpbCounterUpdateReq {bucket = bucket.ToRiakString(), key = counter.ToRiakString(), amount = amount};
            options = options ?? new RiakCounterUpdateOptions();
            options.Populate(request);

            var result = UseConnection(conn => conn.PbcWriteRead<RpbCounterUpdateReq, RpbCounterUpdateResp>(request));

            if (!result.IsSuccess)
            {
                return new RiakCounterResult(RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline), null);
            }

            var o = new RiakObject(bucket, counter, result.Value.value);
            var cVal = 0L;
            var parseResult = false;

            if (options.ReturnValue != null && options.ReturnValue.Value)
                parseResult= long.TryParse(o.Value.FromRiakString(), out cVal);

            return new RiakCounterResult(RiakResult<RiakObject>.Success(o), parseResult ? (long?)cVal : null);
        }

        /// <summary>
        /// Returns the value of a counter
        /// </summary>
        /// <param name="bucket">The bucket</param>
        /// <param name="counter">The counter</param>
        /// <param name="options"><see cref="RiakCounterGetOptions"/> describing how to read the counter.</param>
        /// <returns><see cref="RiakCounterResult"/></returns>
        /// <remarks>Only available in Riak 1.4+.</remarks>
        public RiakCounterResult GetCounter(string bucket, string counter, RiakCounterGetOptions options = null)
        {
            var request = new RpbCounterGetReq {bucket = bucket.ToRiakString(), key = counter.ToRiakString()};
            options = options ?? new RiakCounterGetOptions();
            options.Populate(request);

            var result = UseConnection(conn => conn.PbcWriteRead<RpbCounterGetReq, RpbCounterGetResp>(request));

            if (!result.IsSuccess)
            {
                return new RiakCounterResult(RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline), null);
            }

            var o = new RiakObject(bucket, counter, result.Value.value);
            long cVal;
            var parseResult = long.TryParse(o.Value.FromRiakString(), out cVal);

            return new RiakCounterResult(RiakResult<RiakObject>.Success(o), parseResult ? (long?)cVal : null);
        }

        public RiakResult<RiakObject> Get(string bucketType, string bucket, string key, RiakGetOptions options = null)
        {
            options = options ?? DefaultGetOptions();
            return Get(new RiakObjectId(bucketType, bucket, key), options);
        }

        /// <summary>
        /// Get the specified <paramref name="key"/> from the <paramref name="bucket"/>.
        /// Optionally can be read from rVal instances. By default, the server's
        /// r-value will be used, but can be overridden by rVal.
        /// </summary>
        /// <param name='bucket'>
        /// The name of the bucket containing the <paramref name="key"/>
        /// </param>
        /// <param name='key'>
        /// The key.
        /// </param>
        /// <param name='options'>The <see cref="CorrugatedIron.Models.RiakGetOptions" /> responsible for 
        /// configuring the semantics of this single get request. These options will override any previously 
        /// defined bucket configuration properties.</param>
        /// <remarks>If a node does not respond, that does not necessarily mean that the 
        /// <paramref name="bucket"/>/<paramref name="key"/> combination is not available. It simply means
        /// that fewer than R/PR nodes responded to the read request. See <see cref="CorrugatedIron.Models.RiakGetOptions" />
        /// for information on how different options change Riak's default behavior.
        /// </remarks>
        public RiakResult<RiakObject> Get(string bucket, string key, RiakGetOptions options = null)
        {
            options = options ?? DefaultGetOptions();
            return Get(null, bucket, key, options);
        }

        /// <summary>
        /// Get the specified <paramref name="key"/> from the <paramref name="bucket"/>.
        /// Optionally can be read from <paramref name="rVal"/> instances. By default, the server's
        /// r-value will be used, but can be overridden by <paramref name="rVal"/>.
        /// </summary>
        /// <param name='bucket'>
        /// The name of the bucket containing the <paramref name="key"/>
        /// </param>
        /// <param name='key'>
        /// The key.
        /// </param>
        /// <remarks>If a node does not respond, that does not necessarily mean that the 
        /// <paramref name="bucket"/>/<paramref name="key"/> combination is not available. It simply means
        /// that fewer than the default number nodes responded to the read request. Unfortunatley, 
        /// the Riak API does not allow us to distinguish between a 404 resulting from less than <paramref name="rVal"/>
        /// nodes successfully responding and a <paramref name="bucket"/>/<paramref name="key"/> combination
        /// not being found in Riak.
        /// </remarks>
        public RiakResult<RiakObject> Get(string bucket, string key)
        {
            var options = new RiakGetOptions().SetR(RiakConstants.Defaults.RVal);
            return Get(null, bucket, key, options);
        }

        /// <summary>
        /// Retrieve the specified object from Riak.
        /// </summary>
        /// <param name='objectId'>
        /// Object identifier made up of a key and bucket. <see cref="CorrugatedIron.Models.RiakObjectId"/>
        /// </param>
        /// <param name='options'>The <see cref="CorrugatedIron.Models.RiakGetOptions" /> responsible for 
        /// configuring the semantics of this single get request. These options will override any previously 
        /// defined bucket configuration properties.</param>
        /// <remarks>If a node does not respond, that does not necessarily mean that the 
        /// <paramref name="objectId"/> is not available. It simply means
        /// that fewer than <paramref name="rVal" /> nodes responded to the read request. Unfortunatley, 
        /// the Riak API does not allow us to distinguish between a 404 resulting from less than <paramref name="rVal"/>
        /// nodes successfully responding and an <paramref name="objectId"/> not being found in Riak.
        /// </remarks>
        public RiakResult<RiakObject> Get(RiakObjectId objectId, RiakGetOptions options = null)
        {
            var request = new RpbGetReq { 
                type = objectId.BucketType.ToRiakString(),
                bucket = objectId.Bucket.ToRiakString(), 
                key = objectId.Key.ToRiakString()
            };

            options = options ?? new RiakGetOptions();
            options.Populate(request);

            var result = UseConnection(conn => conn.PbcWriteRead<RpbGetReq, RpbGetResp>(request));

            if(!result.IsSuccess)
            {
                return RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
            }

            if(result.Value.vclock == null)
            {
                return RiakResult<RiakObject>.Error(ResultCode.NotFound, "Unable to find value in Riak", false);
            }

            var o = new RiakObject(objectId.Bucket, objectId.Key, result.Value.content, result.Value.vclock);

            return RiakResult<RiakObject>.Success(o);
        }

        /// <summary>
        /// Retrieve multiple objects from Riak.
        /// </summary>
        /// <param name='objectIds'>
        /// An <see href="System.Collections.Generic.IEnumerable&lt;T&gt;"/> of <see cref="CorrugatedIron.Models.RiakObjectId"/> to be retrieved
        /// </param>
        /// <param name='options'>The <see cref="CorrugatedIron.Models.RiakGetOptions" /> responsible for 
        /// configuring the semantics of this single get request. These options will override any previously 
        /// defined bucket configuration properties.</param>
        /// <returns>An <see cref="System.Collections.Generic.IEnumerable{T}"/> of <see cref="RiakResult{T}"/>
        /// is returned. You should verify the success or failure of each result separately.</returns>
        /// <remarks>Riak does not support multi get behavior. CorrugatedIron's multi get functionality wraps multiple
        /// get requests and returns results as an IEnumerable{RiakResult{RiakObject}}. Callers should be aware that
        /// this may result in partial success - all results should be evaluated individually in the calling application.
        /// In addition, applications should plan for multiple failures or multiple cases of siblings being present.</remarks>
        public IEnumerable<RiakResult<RiakObject>> Get(IEnumerable<RiakObjectId> objectIds,
                                                       RiakGetOptions options = null)
        {
            objectIds = objectIds.ToList();

            options = options ?? new RiakGetOptions();

            var results = UseConnection(conn =>
            {
                var responses = objectIds.Select(bkp =>
                {
                    // modified closure FTW
                    var bk = bkp;

                    var req = new RpbGetReq { 
                        type = bk.BucketType.ToRiakString(),
                        bucket = bk.Bucket.ToRiakString(), 
                        key = bk.Key.ToRiakString() };
                    options.Populate(req);

                    return conn.PbcWriteRead<RpbGetReq, RpbGetResp>(req);
                }).ToList();
                return RiakResult<IEnumerable<RiakResult<RpbGetResp>>>.Success(responses);
            });

            return results.Value.Zip(objectIds, Tuple.Create).Select(result =>
            {
                if(!result.Item1.IsSuccess)
                {
                    return RiakResult<RiakObject>.Error(result.Item1.ResultCode, result.Item1.ErrorMessage, result.Item1.NodeOffline);
                }
                
                if(result.Item1.Value.vclock == null)
                {
                    return RiakResult<RiakObject>.Error(ResultCode.NotFound, "Unable to find value in Riak", false);
                }
                
                var o = new RiakObject(result.Item2.Bucket, result.Item2.Key, result.Item1.Value.content.First(), result.Item1.Value.vclock);
                
                if(result.Item1.Value.content.Count > 1)
                {
                    o.Siblings = result.Item1.Value.content.Select(c =>
                        new RiakObject(result.Item2.Bucket, result.Item2.Key, c, result.Item1.Value.vclock)).ToList();
                }
                
                return RiakResult<RiakObject>.Success(o);
            });
        }

        /// <summary>
        /// Persist a <see cref="CorrugatedIron.Models.RiakObject"/> to Riak using the specific <see cref="CorrugatedIron.Models.RiakPutOptions" />.
        /// </summary>
        /// <param name='value'>
        /// The <see cref="CorrugatedIron.Models.RiakObject"/> to save.
        /// </param>
        /// <param name='options'>
        /// Put options
        /// </param>
        public RiakResult<RiakObject> Put(RiakObject value, RiakPutOptions options = null)
        {
            options = options ?? new RiakPutOptions();

            var request = value.ToMessage();
            options.Populate(request);

            var result = UseConnection(conn => conn.PbcWriteRead<RpbPutReq, RpbPutResp>(request));

            if(!result.IsSuccess)
            {
                return RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
            }

            var finalResult = options.ReturnBody
                ? new RiakObject(value.Bucket, value.Key, result.Value.content.First(), result.Value.vclock)
                : value;

            if(options.ReturnBody && result.Value.content.Count > 1)
            {
                finalResult.Siblings = result.Value.content.Select(c =>
                    new RiakObject(value.Bucket, value.Key, c, result.Value.vclock)).ToList();
            }

            return RiakResult<RiakObject>.Success(finalResult);
        }

        /// <summary>
        /// Persist an <see href="System.Collections.Generic.IEnumerable{T}"/> of <see cref="CorrugatedIron.Models.RiakObjectId"/> to Riak.
        /// </summary>
        /// <param name='values'>
        /// The <see href="System.Collections.Generic.IEnumerable{T}"/> of <see cref="CorrugatedIron.Models.RiakObjectId"/> to save.
        /// </param>
        /// <param name='options'>
        /// Put options.
        /// </param>
        /// <returns>An <see cref="System.Collections.Generic.IEnumerable{T}"/> of <see cref="RiakResult{T}"/>
        /// is returned. You should verify the success or failure of each result separately.</returns>
        /// <remarks>Riak does not support multi put behavior. CorrugatedIron's multi put functionality wraps multiple
        /// put requests and returns results as an IEnumerable{RiakResult{RiakObject}}. Callers should be aware that
        /// this may result in partial success - all results should be evaluated individually in the calling application.
        /// In addition, applications should plan for multiple failures or multiple cases of siblings being present.</remarks>
        public IEnumerable<RiakResult<RiakObject>> Put(IEnumerable<RiakObject> values, RiakPutOptions options = null)
        {
            options = options ?? new RiakPutOptions();

            var results = UseConnection(conn =>
            {
                var responses = values.Select(v =>
                {
                    var msg = v.ToMessage();
                    options.Populate(msg);

                    return conn.PbcWriteRead<RpbPutReq, RpbPutResp>(msg);
                }).ToList();

                return RiakResult<IEnumerable<RiakResult<RpbPutResp>>>.Success(responses);
            });

            return results.Value.Zip(values, Tuple.Create).Select(t =>
            {
                if(t.Item1.IsSuccess)
                {
                    var finalResult = options.ReturnBody
                        ? new RiakObject(t.Item2.BucketType, t.Item2.Bucket, t.Item2.Key, t.Item1.Value.content.First(), t.Item1.Value.vclock)
                        : t.Item2;

                    if(options.ReturnBody && t.Item1.Value.content.Count > 1)
                    {
                        finalResult.Siblings = t.Item1.Value.content.Select(c =>
                            new RiakObject(t.Item2.BucketType, t.Item2.Bucket, t.Item2.Key, c, t.Item1.Value.vclock)).ToList();
                    }

                    return RiakResult<RiakObject>.Success(finalResult);
                }

                return RiakResult<RiakObject>.Error(t.Item1.ResultCode, t.Item1.ErrorMessage, t.Item1.NodeOffline);
            });
        }

        /// <summary>
        /// Delete the data identified by the <paramref name="riakObject"/>
        /// </summary>
        /// <param name='riakObject'>
        /// The object to delete
        /// </param>
        public RiakResult Delete(RiakObject riakObject, RiakDeleteOptions options = null)
        {
            return Delete(riakObject.BucketType, riakObject.Bucket, riakObject.Key, options);
        }

        /// <summary>
        /// Delete the record identified by <paramref name="key"/> from a <paramref name="bucket"/>.
        /// </summary>
        /// <param name='bucket'>
        /// The name of the bucket that contains the record to be deleted.
        /// </param>
        /// <param name='key'>
        /// The key identifying the record to be deleted.
        /// </param>
        /// <param name='options'>
        /// Delete options
        /// </param>
        public RiakResult Delete(string bucket, string key, RiakDeleteOptions options = null)
        {
            return Delete(null, bucket, key, options);
        }

        /// <summary>
        /// Delete the record identified by <paramref name="key"/> from a <paramref name="bucket"/>.
        /// </summary>
        /// <param name='bucketType'>The bucket type</param>
        /// <param name='bucket'>
        /// The name of the bucket that contains the record to be deleted.
        /// </param>
        /// <param name='key'>
        /// The key identifying the record to be deleted.
        /// </param>
        /// <param name='options'>
        /// Delete options
        /// </param>
        public RiakResult Delete(string bucketType, string bucket, string key, RiakDeleteOptions options = null)
        {
            return Delete(new RiakObjectId(bucketType, bucket, key), options);
        }

        /// <summary>
        /// Delete the record identified by the <paramref name="objectId"/>.
        /// </summary>
        /// <param name='objectId'>
        /// A <see cref="CorrugatedIron.Models.RiakObjectId"/> identifying the bucket/key combination for the record to be deleted.
        /// </param>
        /// <param name='options'>
        /// Delete options
        /// </param>
        public RiakResult Delete(RiakObjectId objectId, RiakDeleteOptions options = null)
        {
            options = options ?? new RiakDeleteOptions();

            var request = new RpbDelReq { 
                type = objectId.BucketType.ToRiakString(),
                bucket = objectId.Bucket.ToRiakString(), 
                key = objectId.Key.ToRiakString() 
            };

            options.Populate(request);
            var result = UseConnection(conn => conn.PbcWriteRead(request, MessageCode.DelResp));

            return result;
        }

        /// <summary>
        /// Delete multiple objects identified by a <see cref="System.Collections.Generic.IEnumerable&lt;T&gt;"/> of <see cref="CorrugatedIron.Models.RiakObjectId"/>.
        /// </summary>
        /// <param name='objectIds'>
        /// A <see cref="System.Collections.Generic.IEnumerable&lt;T&gt;"/> of <see cref="CorrugatedIron.Models.RiakObjectId"/>.
        /// </param>
        /// <param name='options'>
        /// Delete options.
        /// </param>    
        public IEnumerable<RiakResult> Delete(IEnumerable<RiakObjectId> objectIds, RiakDeleteOptions options = null)
        {
            var results = UseConnection(conn => Delete(conn, objectIds, options));
            return results.Value;
        }

        /// <summary>
        /// Deletes the contents of the specified <paramref name="bucket"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="System.Collections.Generic.IEnumerable&lt;T&gt;"/> of <see cref="CorrugatedIron.RiakResult"/> listing the success of all deletes
        /// </returns>
        /// <param name='bucket'>
        /// The bucket to be deleted.
        /// </param>
        /// <param name='rwVal'>
        /// The number of nodes that must respond successfully to a delete request.
        /// </param>
        /// <remarks>
        /// /// <para>
        /// Because of the <see cref="CorrugatedIron.RiakClient.ListKeys"/> operation, this may be a time consuming operation on
        /// production systems and may cause memory problems for the client. This should be used either in testing or on small buckets with 
        /// known amounts of data.
        /// </para>
        /// <para>
        /// A delete bucket operation actually deletes all keys in the bucket individually. 
        /// A <see cref="CorrugatedIron.RiakClient.ListKeys"/> operation is performed to retrieve a list of keys
        /// The keys retrieved from the <see cref="CorrugatedIron.RiakClient.ListKeys"/> are then deleted through
        /// <see cref="CorrugatedIron.RiakClient.Delete"/>. 
        /// </para>
        /// </remarks>
        public IEnumerable<RiakResult> DeleteBucket(string bucket, uint rwVal)
        {
            return DeleteBucket(null, bucket, new RiakDeleteOptions().SetRw(rwVal));
        }

        /// <summary>
        /// Deletes the contents of the specified <paramref name="bucket"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="System.Collections.Generic.IEnumerable&lt;T&gt;"/> of <see cref="CorrugatedIron.RiakResult"/> listing the success of all deletes
        /// </returns>
        /// <param name="bucketType">The bucket type</param>
        /// <param name='bucket'>
        /// The bucket to be deleted.
        /// </param>
        /// <param name='rwVal'>
        /// The number of nodes that must respond successfully to a delete request.
        /// </param>
        /// <remarks>
        /// /// <para>
        /// Because of the <see cref="CorrugatedIron.RiakClient.ListKeys"/> operation, this may be a time consuming operation on
        /// production systems and may cause memory problems for the client. This should be used either in testing or on small buckets with 
        /// known amounts of data.
        /// </para>
        /// <para>
        /// A delete bucket operation actually deletes all keys in the bucket individually. 
        /// A <see cref="CorrugatedIron.RiakClient.ListKeys"/> operation is performed to retrieve a list of keys
        /// The keys retrieved from the <see cref="CorrugatedIron.RiakClient.ListKeys"/> are then deleted through
        /// <see cref="CorrugatedIron.RiakClient.Delete"/>. 
        /// </para>
        /// </remarks>
        public IEnumerable<RiakResult> DeleteBucket(string bucketType, string bucket, uint rwVal)
        {
            return DeleteBucket(bucketType, bucket, new RiakDeleteOptions().SetRw(rwVal));
        }

        /// <summary>
        /// Deletes the contents of the specified <paramref name="bucket"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="System.Collections.Generic.IEnumerable&lt;T&gt;"/> of <see cref="CorrugatedIron.RiakResult"/> listing the success of all deletes
        /// </returns>
        /// <param name='bucket'>
        /// The bucket to be deleted.
        /// </param>
        /// <param name='deleteOptions'>
        /// Options for Riak delete operation <see cref="CorrugatedIron.Models.RiakDeleteOptions"/>
        /// </param>
        /// <remarks>
        /// <para>
        /// A delete bucket operation actually deletes all keys in the bucket individually. 
        /// A <see cref="CorrugatedIron.RiakClient.ListKeys"/> operation is performed to retrieve a list of keys
        /// The keys retrieved from the <see cref="CorrugatedIron.RiakClient.ListKeys"/> are then deleted through
        /// <see cref="CorrugatedIron.RiakClient.Delete"/>. 
        /// </para>
        /// <para>
        /// Because of the <see cref="CorrugatedIron.RiakClient.ListKeys"/> operation, this may be a time consuming operation on
        /// production systems and may cause memory problems for the client. This should be used either in testing or on small buckets with 
        /// known amounts of data.
        /// </para>
        /// </remarks>
        public IEnumerable<RiakResult> DeleteBucket(string bucket, RiakDeleteOptions deleteOptions)
        {
            var results = UseConnection(conn =>
                {
                    var keyResults = ListKeys(conn, null, bucket);
                    if (keyResults.IsSuccess)
                    {
                        var objectIds = keyResults.Value.Select(key => new RiakObjectId(bucket, key)).ToList();
                        return Delete(conn, objectIds, deleteOptions);
                    }
                    return RiakResult<IEnumerable<RiakResult>>.Error(keyResults.ResultCode, keyResults.ErrorMessage, keyResults.NodeOffline);
                });

            return results.Value;
        }

        /// <summary>
        /// Deletes the contents of the specified <paramref name="bucket"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="System.Collections.Generic.IEnumerable&lt;T&gt;"/> of <see cref="CorrugatedIron.RiakResult"/> listing the success of all deletes
        /// </returns>
        /// <param name="bucketType">The bucket type.</param>
        /// <param name='bucket'>
        /// The bucket to be deleted.
        /// </param>
        /// <param name='deleteOptions'>
        /// Options for Riak delete operation <see cref="CorrugatedIron.Models.RiakDeleteOptions"/>
        /// </param>
        /// <remarks>
        /// <para>
        /// A delete bucket operation actually deletes all keys in the bucket individually. 
        /// A <see cref="CorrugatedIron.RiakClient.ListKeys"/> operation is performed to retrieve a list of keys
        /// The keys retrieved from the <see cref="CorrugatedIron.RiakClient.ListKeys"/> are then deleted through
        /// <see cref="CorrugatedIron.RiakClient.Delete"/>. 
        /// </para>
        /// <para>
        /// Because of the <see cref="CorrugatedIron.RiakClient.ListKeys"/> operation, this may be a time consuming operation on
        /// production systems and may cause memory problems for the client. This should be used either in testing or on small buckets with 
        /// known amounts of data.
        /// </para>
        /// </remarks>
        public IEnumerable<RiakResult> DeleteBucket(string bucketType, string bucket, RiakDeleteOptions deleteOptions)
        {
            var results = UseConnection(conn => {
                var keyResults = ListKeys(conn, bucketType, bucket);
                if (keyResults.IsSuccess)
                {
                    var objectIds = keyResults.Value.Select(key => new RiakObjectId(bucketType, bucket, key)).ToList();
                    return Delete(conn, objectIds, deleteOptions);
                }

                return RiakResult<IEnumerable<RiakResult>>.Error(keyResults.ResultCode, keyResults.ErrorMessage, keyResults.NodeOffline);
            });

            return results.Value;
        }

        private static RiakResult<IEnumerable<RiakResult>> Delete(IRiakConnection conn,
            IEnumerable<RiakObjectId> objectIds, RiakDeleteOptions options = null)
        {
            options = options ?? new RiakDeleteOptions();

            var responses = objectIds.Select(id =>
                {
                    var req = new RpbDelReq { bucket = id.Bucket.ToRiakString(), key = id.Key.ToRiakString() };
                    options.Populate(req);
                    return conn.PbcWriteRead(req, MessageCode.DelResp);
                }).ToList();

            return RiakResult<IEnumerable<RiakResult>>.Success(responses);
        }

        /// <summary>
        /// Execute a map reduce query.
        /// </summary>
        /// <param name="query">A <see cref="RiakMapReduceQuery"/></param>
        /// <returns>A <see cref="RiakResult"/> of <see cref="RiakMapReduceResult"/></returns>
        public RiakResult<RiakMapReduceResult> MapReduce(RiakMapReduceQuery query)
        {
            var request = query.ToMessage();
            var response = UseConnection(conn => conn.PbcWriteRead<RpbMapRedReq, RpbMapRedResp>(request, r => r.IsSuccess && !r.Value.done));

            if(response.IsSuccess)
            {
                return RiakResult<RiakMapReduceResult>.Success(new RiakMapReduceResult(response.Value));
            }

            return RiakResult<RiakMapReduceResult>.Error(response.ResultCode, response.ErrorMessage, response.NodeOffline);
        }

        /// <summary>
        /// Perform a Riak Search query
        /// </summary>
        /// <param name="search">The <see cref="RiakSearchRequest"/></param>
        /// <returns>A <see cref="RiakResult"/> of <see cref="RiakSearchResult"/></returns>
        public RiakResult<RiakSearchResult> Search(RiakSearchRequest search)
        {
            var request = search.ToMessage();
            var response = UseConnection(conn => conn.PbcWriteRead<RpbSearchQueryReq, RpbSearchQueryResp>(request));

            if (response.IsSuccess)
            {
                return RiakResult<RiakSearchResult>.Success(new RiakSearchResult(response.Value));
            }

            return RiakResult<RiakSearchResult>.Error(response.ResultCode, response.ErrorMessage, response.NodeOffline);
        }

        /// <summary>
        /// Perform a map reduce query and stream the results.
        /// </summary>
        /// <param name="query">The query</param>
        /// <returns>A <see cref="RiakResult"/> of <see cref="RiakStreamedMapReduceResult"/></returns>
        /// <remarks>Make sure to fully enumerate the <see cref="RiakStreamedMapReduceResult"/> or connections may be left open.</remarks>
        public RiakResult<RiakStreamedMapReduceResult> StreamMapReduce(RiakMapReduceQuery query)
        {
            var request = query.ToMessage();
            var response = UseDelayedConnection((conn, onFinish) =>
                conn.PbcWriteStreamRead<RpbMapRedReq, RpbMapRedResp>(request, r => r.IsSuccess && !r.Value.done, onFinish));

            if(response.IsSuccess)
            {
                return RiakResult<RiakStreamedMapReduceResult>.Success(new RiakStreamedMapReduceResult(response.Value));
            }
            return RiakResult<RiakStreamedMapReduceResult>.Error(response.ResultCode, response.ErrorMessage, response.NodeOffline);
        }

        /// <summary>
        /// Lists all buckets available on the Riak cluster.
        /// </summary>
        /// <returns>
        /// An <see cref="System.Collections.Generic.IEnumerable&lt;T&gt;"/> of <see cref="string"/> bucket names.
        /// </returns>
        /// <remarks>Buckets provide a logical namespace for keys. Listing buckets requires folding over all keys in a cluster and 
        /// reading a list of buckets from disk. This operation, while non-blocking in Riak 1.0 and newer, still produces considerable
        /// physical I/O and can take a long time.</remarks>
        public RiakResult<IEnumerable<string>> ListBuckets()
        {
            var result = UseConnection(conn => conn.PbcWriteRead<RpbListBucketsResp>(MessageCode.ListBucketsReq));

            if(result.IsSuccess)
            {
                var buckets = result.Value.buckets.Select(b => b.FromRiakString());
                return RiakResult<IEnumerable<string>>.Success(buckets.ToList());
            }
            return RiakResult<IEnumerable<string>>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        /// <summary>
        /// Lists all buckets available on the Riak cluster. This uses an <see cref="System.Collections.Generic.IEnumerable&lt;T&gt;"/> 
        /// of <see cref="string"/> to lazy initialize the collection of bucket names. 
        /// </summary>
        /// <returns>
        /// An <see cref="System.Collections.Generic.IEnumerable&lt;T&gt;"/> of <see cref="string"/> bucket names.
        /// </returns>
        /// <remarks>Buckets provide a logical namespace for keys. Listing buckets requires folding over all keys in a cluster and 
        /// reading a list of buckets from disk. This operation, while non-blocking in Riak 1.0 and newer, still produces considerable
        /// physical I/O and can take a long time. Callers should fully enumerate the collection or else close the connection when finished.</remarks>
        public RiakResult<IEnumerable<string>> StreamListBuckets()
        {
            var lbReq = new RpbListBucketsReq { stream = true };
            var result = UseDelayedConnection((conn, onFinish) =>
                                              conn.PbcWriteStreamRead<RpbListBucketsReq, RpbListBucketsResp>(lbReq, lbr => lbr.IsSuccess && !lbr.Value.done, onFinish));

            if(result.IsSuccess)
            {
                var buckets = result.Value.Where(r => r.IsSuccess).SelectMany(r => r.Value.buckets).Select(k => k.FromRiakString());
                return RiakResult<IEnumerable<string>>.Success(buckets);
            }

            return RiakResult<IEnumerable<string>>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        /// <summary>
        /// Lists all keys in the specified <paramref name="bucket"/>.
        /// </summary>
        /// <returns>
        /// The keys.
        /// </returns>
        /// <param name='bucket'>
        /// The bucket.
        /// </param>
        /// <remarks>ListKeys is an expensive operation that requires folding over all data in the Riak cluster to produce
        /// a list of keys. This operation, while cheaper in Riak 1.0 than in earlier versions of Riak, should be avoided.</remarks>
        public RiakResult<IEnumerable<string>> ListKeys(string bucket)
        {
            return UseConnection(conn => ListKeys(conn, null, bucket));
        }

        /// <summary>
        /// Lists all keys in the specified <paramref name="bucket"/>.
        /// </summary>
        /// <returns>
        /// The keys.
        /// </returns>
        /// <param name="bucketType">The bucket type - the default value is null. Riak 1.4 users should use null.</param>
        /// <param name='bucket'>
        /// The bucket.
        /// </param>
        /// <remarks>ListKeys is an expensive operation that requires folding over all data in the Riak cluster to produce
        /// a list of keys. This operation, while cheaper in Riak 1.0 than in earlier versions of Riak, should be avoided.</remarks>
        public RiakResult<IEnumerable<string>> ListKeys(string bucketType, string bucket)
        {
            return UseConnection(conn => ListKeys(conn, bucketType, bucket));
        }

        private static RiakResult<IEnumerable<string>> ListKeys(IRiakConnection conn, string bucketType, string bucket)
        {
            System.Diagnostics.Debug.Write(ListKeysWarning);
            System.Diagnostics.Trace.TraceWarning(ListKeysWarning);
            Console.WriteLine(ListKeysWarning);

            var lkReq = new RpbListKeysReq { 
                type = bucketType.ToRiakString(),
                bucket = bucket.ToRiakString() 
            };
            var result = conn.PbcWriteRead<RpbListKeysReq, RpbListKeysResp>(lkReq,
                lkr => lkr.IsSuccess && !lkr.Value.done);
            if(result.IsSuccess)
            {
                var keys = result.Value.Where(r => r.IsSuccess).SelectMany(r => r.Value.keys).Select(k => k.FromRiakString()).Distinct().ToList();
                return RiakResult<IEnumerable<string>>.Success(keys);
            }
            return RiakResult<IEnumerable<string>>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        /// <summary>
        /// Performs a streaming list keys operation.
        /// </summary>
        /// <param name="bucket">The bucket</param>
        /// <returns>An <see cref="System.Collections.Generic.IEnumerable{T}"/></returns>
        /// <remarks>While this streams results back to the client, alleviating pressure on Riak, this still relies on
        /// folding over all keys present in the Riak cluster. Use at your own risk. A better approach would be to
        /// use <see cref="ListKeysFromIndex"/></remarks>
        public RiakResult<IEnumerable<string>> StreamListKeys(string bucket)
        {
            return StreamListKeys(null, bucket);
        }

        public RiakResult<IEnumerable<string>> StreamListKeys(string bucketType, string bucket)
        {
            System.Diagnostics.Debug.Write(ListKeysWarning);
            System.Diagnostics.Trace.TraceWarning(ListKeysWarning);
            Console.WriteLine(ListKeysWarning);

            var lkReq = new RpbListKeysReq { 
                type = bucketType.ToRiakString(),
                bucket = bucket.ToRiakString() 
            };
            var result = UseDelayedConnection((conn, onFinish) =>
                                              conn.PbcWriteStreamRead<RpbListKeysReq, RpbListKeysResp>(lkReq, lkr => lkr.IsSuccess && !lkr.Value.done, onFinish));

            if(result.IsSuccess)
            {
                var keys = result.Value.Where(r => r.IsSuccess).SelectMany(r => r.Value.keys).Select(k => k.FromRiakString());
                return RiakResult<IEnumerable<string>>.Success(keys);
            }
            return RiakResult<IEnumerable<string>>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }      

        /// <summary>
        /// Return a list of keys from the given bucket.
        /// </summary>
        /// <param name="bucket"></param>
        /// <returns></returns>
        /// <remarks>This uses the $key special index instead of the list keys API to 
        /// quickly return an unsorted list of keys from Riak.</remarks>
        public RiakResult<IList<string>> ListKeysFromIndex(string bucket)
        {
            return ListKeysFromIndex(null, bucket);
        }

        public RiakResult<IList<string>> ListKeysFromIndex(string bucketType, string bucket)
        {
            var result = GetSecondaryIndex(new RiakIndexId(bucketType, bucket, RiakConstants.SystemIndexKeys.RiakBucketIndex), bucket);
            return RiakResult<IList<string>>.Success(result.Value.IndexKeyTerms.Select(ikt => ikt.Key).ToList());
        }

        /// <summary>
        /// Returns all properties for a <paramref name="bucket"/>.
        /// </summary>
        /// <returns>
        /// The bucket properties.
        /// </returns>
        /// <param name='bucket'>
        /// The Riak bucket.
        /// </param>
        public RiakResult<RiakBucketProperties> GetBucketProperties(string bucket)
        {
            return GetBucketProperties(null, bucket);
        }

        public RiakResult<RiakBucketProperties> GetBucketProperties(string bucketType, string bucket)
        {
            var bpReq = new RpbGetBucketReq { type = bucketType.ToRiakString(), bucket = bucket.ToRiakString() };
            var result = UseConnection(conn => conn.PbcWriteRead<RpbGetBucketReq, RpbGetBucketResp>(bpReq));

            if(result.IsSuccess)
            {
                var props = new RiakBucketProperties(result.Value.props);
                return RiakResult<RiakBucketProperties>.Success(props);
            }
            return RiakResult<RiakBucketProperties>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        /// <summary>
        /// Sets the <see cref="CorrugatedIron.Models.RiakBucketProperties"/> properties of a <paramref name="bucket"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="CorrugatedIron.RiakResult"/> detailing the success or failure of the operation.
        /// </returns>
        /// <param name='bucket'>
        /// The Bucket.
        /// </param>
        /// <param name='properties'>
        /// The Properties.
        /// </param>
        /// <param name='useHttp'>When true, CorrugatedIron will use the HTTP interface</param>
        /// <remarks>There is, as of CorrugatedIron 2.0, no reason to use the HTTP interface. This is kept for legacy reasons.</remarks>
        public RiakResult SetBucketProperties(string bucket, RiakBucketProperties properties, bool useHttp = false)
        {
            return useHttp ? SetHttpBucketProperties(bucket, properties) : SetBucketProperties(null, bucket, properties);
        }

        public RiakResult SetBucketProperties(string bucketType, string bucket, RiakBucketProperties properties)
        {
            return SetPbcBucketProperties(bucketType, bucket, properties);
        }

        internal RiakResult SetHttpBucketProperties(string bucket, RiakBucketProperties properties)
        {
            var request = new RiakRestRequest(ToBucketUri(bucket), RiakConstants.Rest.HttpMethod.Put)
            {
                Body = properties.ToJsonString().ToRiakString(),
                ContentType = RiakConstants.ContentTypes.ApplicationJson
            };

            var result = UseConnection(conn => conn.RestRequest(request));
            if(result.IsSuccess && result.Value.StatusCode != HttpStatusCode.NoContent)
            {
                return RiakResult.Error(ResultCode.InvalidResponse, "Unexpected Status Code: {0} ({1})".Fmt(result.Value.StatusCode, (int)result.Value.StatusCode), result.NodeOffline);
            }

            return result;
        }

        internal RiakResult SetPbcBucketProperties(string bucketType, string bucket, RiakBucketProperties properties)
        {
            var request = new RpbSetBucketReq { 
                type = bucketType.ToRiakString(), 
                bucket = bucket.ToRiakString(), 
                props = properties.ToMessage() 
            };
            var result = UseConnection(conn => conn.PbcWriteRead(request, MessageCode.SetBucketResp));

            return result;
        }

        /// <summary>
        /// Reset the properties on a bucket back to their defaults.
        /// </summary>
        /// <param name="bucket">The name of the bucket to reset the properties on.</param>
        /// <param name="useHttp">Whether or not to use the HTTP interface to Riak. Set to true for Riak 1.3 and earlier</param> 
        /// <returns>An indication of success or failure.</returns>
        /// <remarks>There is, as of CorrugatedIron 2.0, no reason to use the HTTP interface. This is kept for legacy reasons.</remarks>
        public RiakResult ResetBucketProperties(string bucket, bool useHttp = false)
        {
            return useHttp ? ResetHttpBucketProperties(bucket) : ResetPbcBucketProperties(null, bucket);
        }

        /// <summary>
        /// Reset the properties on a bucket back to their defaults.
        /// </summary>
        /// <param name="bucket">The name of the bucket to reset the properties on.</param>
        /// <param name="bucketType">The bucket type. Bucket types are a Riak 2.0 feature and do not apply to Riak 1.4 and earlier</param>
        /// <returns>An indication of success or failure.</returns>
        public RiakResult ResetBucketProperties(string bucketType, string bucket)
        {
            return ResetPbcBucketProperties(bucketType, bucket);
        }

        internal RiakResult ResetPbcBucketProperties(string bucketType, string bucket)
        {
            var request = new RpbResetBucketReq { 
                type = bucketType.ToRiakString(),
                bucket = bucket.ToRiakString() 
            };
            var result = UseConnection(conn => conn.PbcWriteRead(request, MessageCode.ResetBucketResp));
            return result;
        }

        internal RiakResult ResetHttpBucketProperties(string bucket)
        {
            var request = new RiakRestRequest(ToBucketPropsUri(bucket), RiakConstants.Rest.HttpMethod.Delete);

            var result = UseConnection(conn => conn.RestRequest(request));
            if(result.IsSuccess)
            {
                switch (result.Value.StatusCode)
                {
                    case HttpStatusCode.NoContent:
                    return result;
                    case HttpStatusCode.NotFound:
                    return RiakResult.Error(ResultCode.NotFound, "Bucket {0} not found.".Fmt(bucket), false);
                    default:
                    return RiakResult.Error(ResultCode.InvalidResponse, "Unexpected Status Code: {0} ({1})".Fmt(result.Value.StatusCode, (int)result.Value.StatusCode), result.NodeOffline);
                }
            }
            return result;
        }

        /// <summary>
        /// Retrieve arbitrarily deep list of links for a <see cref="RiakObject"/>
        /// </summary>
        /// <returns>
        /// A list of <see cref="RiakObject"/> identified by the list of links.
        /// </returns>
        /// <param name='riakObject'>
        /// The initial object to use for the beginning of the link walking.
        /// </param>
        /// <param name='riakLinks'>
        /// A list of link definitions
        /// </param>
        /// <remarks>Refer to http://wiki.basho.com/Links-and-Link-Walking.html for more information.</remarks>
        public RiakResult<IList<RiakObject>> WalkLinks(RiakObject riakObject, IList<RiakLink> riakLinks)
        {
            System.Diagnostics.Debug.Assert(riakLinks.Count > 0, "Link walking requires at least one link");

            var input = new RiakBucketKeyInput()
                .Add(riakObject.Bucket, riakObject.Key);

            var query = new RiakMapReduceQuery()
                .Inputs(input);

            var lastLink = riakLinks.Last();

            foreach(var riakLink in riakLinks)
            {
                var link = riakLink;
                var keep = ReferenceEquals(link, lastLink);

                query.Link(l => l.FromRiakLink(link).Keep(keep));
            }

            var result = MapReduce(query);

            if(result.IsSuccess)
            {
                var linkResults = result.Value.PhaseResults.GroupBy(r => r.Phase).Where(g => g.Key == riakLinks.Count - 1);
                var linkResultStrings = linkResults.SelectMany(lr => lr.ToList(), (lr, r) => new { lr, r })
                    .SelectMany(@t => @t.r.Values, (@t, s) => s.FromRiakString());

                //var linkResultStrings = linkResults.SelectMany(g => g.Select(r => r.Values.Value.FromRiakString()));
                var rawLinks = linkResultStrings.SelectMany(RiakLink.ParseArrayFromJsonString).Distinct();
                var oids = rawLinks.Select(l => new RiakObjectId(l.Bucket, l.Key)).ToList();

                var objects = Get(oids, new RiakGetOptions());

                // FIXME
                // we could be discarding results here. Not good?
                // This really should be a multi-phase map/reduce
                return RiakResult<IList<RiakObject>>.Success(objects.Where(r => r.IsSuccess).Select(r => r.Value).ToList());
            }
            return RiakResult<IList<RiakObject>>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        /// <summary>
        /// Get the server information from the connected cluster.
        /// </summary>
        /// <returns>Model containing information gathered from a node in the cluster.</returns>
        /// <remarks>This function will assume that all of the nodes in the cluster are running
        /// the same version of Riak. It will only get executed on a single node, and the content
        /// that is returned technically only relates to that node. All nodes in a cluster should
        /// run on the same version of Riak.</remarks>
        public RiakResult<RiakServerInfo> GetServerInfo()
        {
            var result = UseConnection(conn => conn.PbcWriteRead<RpbGetServerInfoResp>(MessageCode.GetServerInfoReq));

            if(result.IsSuccess)
            {
                return RiakResult<RiakServerInfo>.Success(new RiakServerInfo(result.Value));
            }
            return RiakResult<RiakServerInfo>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        /// <summary>
        /// Retrieve index results using the streaming interface.
        /// </summary>
        /// <param name="bucket">The bucket</param>
        /// <param name="indexName">The index</param>
        /// <param name="value">The indexed value to search for</param>
        /// <param name="options">The <see cref="RiakIndexGetOptions"/></param>
        /// <returns>A <see cref="RiakResult{T}"/> of <see cref="RiakStreamedIndexResult"/> containing an <see cref="IEnumerable{T}"/>
        /// of <see cref="RiakIndexKeyTerm"/></returns>
        /// <remarks>Make sure to fully enumerate the <see cref="IEnumerable{T}"/> of <see cref="RiakIndexKeyTerm"/>.</remarks>
        [Obsolete("Use StreamGetSecondaryIndex(RiakIndexId, BigInteger, RiakIndexGetOptions) instead")] 
        public RiakResult<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, BigInteger value, RiakIndexGetOptions options = null)
        {
            return StreamIndexGetEquals(bucket, indexName.ToIntegerKey(), value.ToString(), options);
        }

        /// <summary>
        /// Retrieve index results using the streaming interface.
        /// </summary>
        /// <param name="bucket">The bucket</param>
        /// <param name="indexName">The index</param>
        /// <param name="value">The indexed value to search for</param>
        /// <param name="options">The <see cref="RiakIndexGetOptions"/></param>
        /// <returns>A <see cref="RiakResult{T}"/> of <see cref="RiakStreamedIndexResult"/> containing an <see cref="IEnumerable{T}"/>
        /// of <see cref="RiakIndexKeyTerm"/></returns>
        /// <remarks>Make sure to fully enumerate the <see cref="IEnumerable{T}"/> of <see cref="RiakIndexKeyTerm"/>.</remarks>
        [Obsolete("Use StreamGetSecondaryIndex(RiakIndexId, string, RiakIndexGetOptions) instead")] 
        public RiakResult<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, string value, RiakIndexGetOptions options = null)
        {
            return StreamIndexGetEquals(bucket, indexName.ToBinaryKey(), value, options);
        }

        private RiakResult<RiakStreamedIndexResult> StreamIndexGetEquals(string bucket, string indexName, string value,
                                                                        RiakIndexGetOptions options = null)
        {
            return StreamGetSecondaryIndexEquals(new RiakIndexId(bucket, indexName), value, options);
        }

        public RiakResult<RiakStreamedIndexResult> StreamGetSecondaryIndex(RiakIndexId index, BigInteger value, RiakIndexGetOptions options = null)
        {
            index.SetIndexName(index.IndexName.ToIntegerKey());
            return StreamGetSecondaryIndexEquals(index, value.ToString(), options);
        }

        public RiakResult<RiakStreamedIndexResult> StreamGetSecondaryIndex(RiakIndexId index, string value, RiakIndexGetOptions options = null)
        {
            index.SetIndexName(index.IndexName.ToBinaryKey());
            return StreamGetSecondaryIndexEquals(index, value, options);
        }

        private RiakResult<RiakStreamedIndexResult> StreamGetSecondaryIndexEquals(RiakIndexId index, string value, RiakIndexGetOptions options = null)
        {
            var message = new RpbIndexReq {
                type = index.BucketType.ToRiakString(),
                bucket = index.BucketName.ToRiakString(),
                index = index.IndexName.ToRiakString(),
                qtype = RpbIndexReq.IndexQueryType.eq,
                key = value.ToRiakString(),
                stream = true
            };

            return StreamingIndexRead(message, options);
        }


        /// <summary>
        /// Retrieve index results using the streaming interface.
        /// </summary>
        /// <param name="bucket">The bucket</param>
        /// <param name="indexName">The index</param>
        /// <param name="minValue">The start of the indexed range to search for</param>
        /// <param name="maxValue">The end of the indexed range to search for</param>
        /// <param name="options">The <see cref="RiakIndexGetOptions"/></param>
        /// <returns>A <see cref="RiakResult{T}"/> of <see cref="RiakStreamedIndexResult"/> containing an <see cref="IEnumerable{T}"/>
        /// of <see cref="RiakIndexKeyTerm"/></returns>
        /// <remarks>Make sure to fully enumerate the <see cref="IEnumerable{T}"/> of <see cref="RiakIndexKeyTerm"/>.</remarks>
        [Obsolete("Use StreamGetSecondaryIndex(RiakIndexId, BigInteger, BigInteger, RiakIndexGetOptions) instead")]
        public RiakResult<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, BigInteger minValue, BigInteger maxValue, RiakIndexGetOptions options = null)
        {
            return StreamIndexGetRange(bucket, indexName.ToIntegerKey(), minValue.ToString(), maxValue.ToString(), options);
        }

        /// <summary>
        /// Retrieve index results using the streaming interface.
        /// </summary>
        /// <param name="bucket">The bucket</param>
        /// <param name="indexName">The index</param>
        /// <param name="minValue">The start of the indexed range to search for</param>
        /// <param name="maxValue">The end of the indexed range to search for</param>
        /// <param name="options">The <see cref="RiakIndexGetOptions"/></param>
        /// <returns>A <see cref="RiakResult{T}"/> of <see cref="RiakStreamedIndexResult"/> containing an <see cref="IEnumerable{T}"/>
        /// of <see cref="RiakIndexKeyTerm"/></returns>
        /// <remarks>Make sure to fully enumerate the <see cref="IEnumerable{T}"/> of <see cref="RiakIndexKeyTerm"/>.</remarks>
        [Obsolete("Use StreamGetSecondaryIndex(RiakIndexId, string, string, RiakIndexGetOptions) instead")]
        public RiakResult<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null)
        {
            return StreamIndexGetRange(bucket, indexName.ToBinaryKey(), minValue, maxValue, options);
        }

        private RiakResult<RiakStreamedIndexResult> StreamIndexGetRange(string bucket, string indexName, string minValue, string maxValue,
                                         RiakIndexGetOptions options = null)
        {
            return StreamGetSecondaryIndexRange(new RiakIndexId(bucket, indexName), minValue, maxValue, options);
        }



        public RiakResult<RiakStreamedIndexResult> StreamGetSecondaryIndex(RiakIndexId index, BigInteger min, BigInteger max, RiakIndexGetOptions options = null)
        {
            index.SetIndexName(index.IndexName.ToIntegerKey());
            return StreamGetSecondaryIndexRange(index, min.ToString(), max.ToString(), options);
        }

        public RiakResult<RiakStreamedIndexResult> StreamGetSecondaryIndex(RiakIndexId index, string min, string max, RiakIndexGetOptions options = null)
        {
            index.SetIndexName(index.IndexName.ToBinaryKey());
            return StreamGetSecondaryIndexRange(index, min, max, options);
        }

        private RiakResult<RiakStreamedIndexResult> StreamGetSecondaryIndexRange(RiakIndexId index, string min, string max, 
                                                                                 RiakIndexGetOptions options = null)
        {
            var message = new RpbIndexReq {
                type = index.BucketType.ToRiakString(),
                bucket = index.BucketName.ToRiakString(),
                index = index.IndexName.ToRiakString(),
                qtype = RpbIndexReq.IndexQueryType.range,
                range_min = min.ToRiakString(),
                range_max = max.ToRiakString(),
                stream = true
            };

            return StreamingIndexRead(message, options);
        }

        private RiakResult<RiakStreamedIndexResult> StreamingIndexRead(RpbIndexReq message, RiakIndexGetOptions options)
        {
            options = options ?? new RiakIndexGetOptions();
            options.Populate(message);
            var result = UseDelayedConnection((conn, onFinish) => conn.PbcWriteStreamRead<RpbIndexReq, RpbIndexResp>(message, lbr => lbr.IsSuccess && !lbr.Value.done, onFinish));
            if (result.IsSuccess)
            {
                return RiakResult<RiakStreamedIndexResult>.Success(new RiakStreamedIndexResult(ReturnTerms(options), result.Value));
            }
            return RiakResult<RiakStreamedIndexResult>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        private static bool ReturnTerms(RiakIndexGetOptions options)
        {
            return options.ReturnTerms != null && options.ReturnTerms.Value;
        }

        /// <summary>
        /// Retrieve a range of indexed values.
        /// </summary>
        /// <param name="bucket">The bucket</param>
        /// <param name="indexName">The index</param>
        /// <param name="minValue">The start of the indexed range to search for</param>
        /// <param name="maxValue">The end of the indexed range to search for</param>
        /// <param name="options">The <see cref="RiakIndexGetOptions"/></param>
        /// <returns>A <see cref="RiakResult{T}"/> of <see cref="RiakIndexResult"/></returns>
        [Obsolete("Use GetSecondaryIndex instead")]
        public RiakResult<RiakIndexResult> IndexGet(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null)
        {
            return IndexGetRange(bucket, indexName.ToBinaryKey(), minValue, maxValue, options);
        }

        /// <summary>
        /// Retrieve a range of indexed values.
        /// </summary>
        /// <param name="bucket">The bucket</param>
        /// <param name="indexName">The index</param>
        /// <param name="minValue">The start of the indexed range to search for</param>
        /// <param name="maxValue">The end of the indexed range to search for</param>
        /// <param name="options">The <see cref="RiakIndexGetOptions"/></param>
        /// <returns>A <see cref="RiakResult{T}"/> of <see cref="RiakIndexResult"/></returns>
        [Obsolete("Use GetSecondaryIndex instead")]
        public RiakResult<RiakIndexResult> IndexGet(string bucket, string indexName, BigInteger minValue, BigInteger maxValue, RiakIndexGetOptions options = null)
        {
            return IndexGetRange(bucket, indexName.ToIntegerKey(), minValue.ToString(), maxValue.ToString(), options);
        }

        [Obsolete("Use GetSecondaryIndex instead")]
        private RiakResult<RiakIndexResult> IndexGetRange(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null)
        {
            return GetSecondaryIndexRange(new RiakIndexId(bucket, indexName), minValue, maxValue, options);
        }

        public RiakResult<RiakIndexResult> GetSecondaryIndex(RiakIndexId index, BigInteger minValue, BigInteger maxValue, RiakIndexGetOptions options = null)
        {
            return GetSecondaryIndexRange(index, minValue.ToString(), maxValue.ToString(), options);
        }

        public RiakResult<RiakIndexResult> GetSecondaryIndex(RiakIndexId index, string minValue, string maxValue, RiakIndexGetOptions options = null)
        {
            return GetSecondaryIndexRange(index, minValue, maxValue, options);
        }

        private RiakResult<RiakIndexResult> GetSecondaryIndexRange(RiakIndexId index, string minValue, string maxValue, RiakIndexGetOptions options = null)
        {
            var message = new RpbIndexReq
            {
                type = index.BucketType.ToRiakString(),
                bucket = index.BucketName.ToRiakString(),
                index = index.IndexName.ToRiakString(),
                qtype = RpbIndexReq.IndexQueryType.range,
                range_min = minValue.ToRiakString(),
                range_max = maxValue.ToRiakString()
            };

            options = options ?? new RiakIndexGetOptions();
            options.Populate(message);

            var result = UseConnection(conn => conn.PbcWriteRead<RpbIndexReq, RpbIndexResp>(message));

            if (result.IsSuccess)
            {
                var r = RiakResult<RiakIndexResult>.Success(new RiakIndexResult(ReturnTerms(options), result));

                if (result.Done.HasValue)
                    r.SetDone(result.Done.Value);

                if (result.Value.continuation != null)
                {
                    var continuation = result.Value.continuation.FromRiakString();

                    if (!string.IsNullOrEmpty(continuation))
                        r.SetContinuation(continuation);
                }

                return r;
            }

            return RiakResult<RiakIndexResult>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }

        /// <summary>
        /// Retrieve a indexed values
        /// </summary>
        /// <param name="bucket">The bucket</param>
        /// <param name="indexName">The index</param>
        /// <param name="value">The indexed value to search for</param>
        /// <param name="options">The <see cref="RiakIndexGetOptions"/></param>
        /// <returns>A <see cref="RiakResult{T}"/> of <see cref="RiakIndexResult"/></returns>
        [Obsolete("Use GetSecondaryIndex instead")]
        public RiakResult<RiakIndexResult> IndexGet(string bucket, string indexName, string value, RiakIndexGetOptions options = null)
        {
            return IndexGetEquals(bucket, indexName.ToBinaryKey(), value, options);
        }

        /// <summary>
        /// Retrieve an indexed values
        /// </summary>
        /// <param name="bucket">The bucket</param>
        /// <param name="indexName">The index</param>
        /// <param name="value">The indexed value to search for</param>
        /// <param name="options">The <see cref="RiakIndexGetOptions"/></param>
        /// <returns>A <see cref="RiakResult{T}"/> of <see cref="RiakIndexResult"/></returns>
        [Obsolete("Use GetSecondaryIndex instead")]
        public RiakResult<RiakIndexResult> IndexGet(string bucket, string indexName, BigInteger value, RiakIndexGetOptions options = null)
        {
            return IndexGetEquals(bucket, indexName.ToIntegerKey(), value.ToString(), options);
        }

        [Obsolete("Use GetSecondaryIndex instead")]
        private RiakResult<RiakIndexResult> IndexGetEquals(string bucket, string indexName, string value, RiakIndexGetOptions options = null)
        {
            return GetSecondaryIndex(new RiakIndexId(bucket, indexName), value, options);
        }

        /// <summary>
        /// Retrieve an indexed values
        /// </summary>
        /// <param name="index">The bucket type, bucket, index combination representing the index</param>
        /// <param name="value">The indexed value to search for</param>
        /// <param name="options">The <see cref="RiakIndexGetOptions"/></param>
        /// <returns>A <see cref="RiakResult{T}"/> of <see cref="RiakIndexResult"/></returns>
        public RiakResult<RiakIndexResult> GetSecondaryIndex(RiakIndexId index, BigInteger value, RiakIndexGetOptions options = null)
        {
            return GetSecondaryIndexEquals(index, value.ToString(), options);
        }

        /// <summary>
        /// Retrieve an indexed values
        /// </summary>
        /// <param name="index">The bucket type, bucket, index combination representing the index</param>
        /// <param name="value">The indexed value to search for</param>
        /// <param name="options">The <see cref="RiakIndexGetOptions"/></param>
        /// <returns>A <see cref="RiakResult{T}"/> of <see cref="RiakIndexResult"/></returns>
        public RiakResult<RiakIndexResult> GetSecondaryIndex(RiakIndexId index, string value, RiakIndexGetOptions options = null)
        {
            return GetSecondaryIndexEquals(index, value, options);
        }

        private RiakResult<RiakIndexResult> GetSecondaryIndexEquals(RiakIndexId index, string value, RiakIndexGetOptions options = null)
        {
            var message = new RpbIndexReq {
                type = index.BucketType.ToRiakString(),
                bucket = index.BucketName.ToRiakString(),
                index = index.IndexName.ToRiakString(),
                key = value.ToRiakString(),
                qtype = RpbIndexReq.IndexQueryType.eq
            };

            options = options ?? new RiakIndexGetOptions();
            options.Populate(message);

            var result = UseConnection(conn => conn.PbcWriteRead<RpbIndexReq, RpbIndexResp>(message));

            if (result.IsSuccess)
            {
                return RiakResult<RiakIndexResult>.Success(new RiakIndexResult(ReturnTerms(options), result));
            }

            return RiakResult<RiakIndexResult>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }



        /// <summary>
        /// Used to create a batched set of actions to be sent to a Riak cluster. This guarantees some level of serialized activity.
        /// </summary>
        /// <param name='batchAction'>
        /// Batch action.
        /// </param>
        /// <exception cref='Exception'>
        /// Represents errors that occur during application execution.
        /// </exception>
        public void Batch(Action<IRiakBatchClient> batchAction)
        {
            Batch<object>(c => { batchAction(c); return null; });
        }

        public T Batch<T>(Func<IRiakBatchClient, T> batchFun)
        {
            var funResult = default(T);

            Func<IRiakConnection, Action, RiakResult<IEnumerable<RiakResult<object>>>> helperBatchFun = (conn, onFinish) =>
            {
                try
                {
                    funResult = batchFun(new RiakClient(conn));
                    return RiakResult<IEnumerable<RiakResult<object>>>.Success(null);
                }
                catch(Exception ex)
                {
                    return RiakResult<IEnumerable<RiakResult<object>>>.Error(ResultCode.BatchException, "{0}\n{1}".Fmt(ex.Message, ex.StackTrace), true);
                }
                finally
                {
                    onFinish();
                }
            };

            var result = _endPoint.UseDelayedConnection(helperBatchFun, RetryCount);

            if(!result.IsSuccess && result.ResultCode == ResultCode.BatchException)
            {
                throw new Exception(result.ErrorMessage);
            }

            return funResult;
        }

        private RiakResult UseConnection(Func<IRiakConnection, RiakResult> op)
        {
            return _batchConnection != null ? op(_batchConnection) : _endPoint.UseConnection(op, RetryCount);
        }

        private RiakResult<TResult> UseConnection<TResult>(Func<IRiakConnection, RiakResult<TResult>> op)
        {
            return _batchConnection != null ? op(_batchConnection) : _endPoint.UseConnection(op, RetryCount);
        }

        private RiakResult<IEnumerable<RiakResult<TResult>>> UseDelayedConnection<TResult>(
            Func<IRiakConnection, Action, RiakResult<IEnumerable<RiakResult<TResult>>>> op)
        {
            return _batchConnection != null
                ? op(_batchConnection, () => { })
                : _endPoint.UseDelayedConnection(op, RetryCount);
        }

        private static string ToBucketUri(string bucket)
        {
            return "{0}/{1}".Fmt(RiakConstants.Rest.Uri.RiakRoot, HttpUtility.UrlEncode(bucket));
        }

        private static string ToBucketPropsUri(string bucket)
        {
            return RiakConstants.Rest.Uri.BucketPropsFmt.Fmt(HttpUtility.UrlEncode(bucket));
        }

        internal static RiakGetOptions DefaultGetOptions()
        {
            return (new RiakGetOptions()).SetR(RiakConstants.Defaults.RVal);
        }

        public RiakResult<RiakObject> DtFetch(string bucketType, string bucket, string key,
                                              RiakDtFetchOptions options = null)
        {
            return DtFetch(new RiakObjectId(bucketType, bucket, key), options);
        }

        public RiakResult<RiakObject> DtFetch(RiakObjectId riakObject, RiakDtFetchOptions options = null)
        {
            var message = new DtFetchReq
            {
                type = riakObject.BucketType.ToRiakString(),
                bucket = riakObject.Bucket.ToRiakString(),
                key = riakObject.Key.ToRiakString()
            };

            options = options ?? new RiakDtFetchOptions();

            options.Populate(message);

            var result = UseConnection(conn => conn.PbcWriteRead<DtFetchReq, DtFetchResp>(message));

            return result.IsSuccess ? RiakResult<RiakObject>.Success(new RiakObject(new RiakObjectId(riakObject.BucketType, 
                                                                                                     riakObject.Bucket, 
                                                                                                     riakObject.Key), 
                                                                                    result)) 
                : RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline);
        }


        public RiakCounterResult DtFetchCounter(string bucketType, string bucket, string key,
                                                RiakDtFetchOptions options = null)
        {
            return DtFetchCounter(new RiakObjectId(bucketType, bucket, key), options);
        }

        public RiakCounterResult DtFetchCounter(RiakObjectId objectId, RiakDtFetchOptions options = null)
        {
            var message = new DtFetchReq {
                type = objectId.BucketType.ToRiakString(),
                bucket = objectId.Bucket.ToRiakString(),
                key = objectId.Key.ToRiakString()
            };

            options = options ?? new RiakDtFetchOptions();

            options.Populate(message);

            var result = UseConnection(conn => conn.PbcWriteRead<DtFetchReq, DtFetchResp>(message));

            if (!result.IsSuccess)
            {
                return new RiakCounterResult(RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline), null);
            }

            var o = new RiakObject(objectId, result.Value.value);

            var rcr = new RiakCounterResult(RiakResult<RiakObject>.Success(o));

            if (options.IncludeContext.HasValue && options.IncludeContext.Value)
                rcr.Context = result.Value.context;

            return rcr;
        }

        public RiakCounterResult DtUpdateCounter(string bucketType, string bucket, string key,
                                                 long amount,
                                                 byte[] context,
                                                 RiakDtUpdateOptions options = null)
        {
            return DtUpdateCounter(new RiakObjectId(bucketType, bucket, key), amount, context, options);
        }

        public RiakCounterResult DtUpdateCounter(RiakObjectId objectId, long amount, 
                                                 byte[] context, RiakDtUpdateOptions options = null)
        {
            var request = new DtUpdateReq
            {
                type = objectId.BucketType.ToRiakString(),
                bucket = objectId.Bucket.ToRiakString(), 
                key = objectId.Key.ToRiakString()
            };

            options = options ?? new RiakDtUpdateOptions();
            options.Populate(request);

            if (context != null)
                request.context = context;

            var result = UseConnection(conn => conn.PbcWriteRead<DtUpdateReq, DtUpdateResp>(request));

            if (!result.IsSuccess)
            {
                return new RiakCounterResult(RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline), null);
            }

            var o = new RiakObject(objectId, result.Value.counter_value);

            var rcr = new RiakCounterResult(RiakResult<RiakObject>.Success(o));

            if (options.IncludeContext)
                rcr.Context = result.Value.context;

            if (options.ReturnBody)
                rcr.Value = result.Value.counter_value;

            return rcr;
        }

        public RiakDtSetResult DtFetchSet(string bucketType, string bucket, string key, RiakDtFetchOptions options = null)
        {
            return DtFetchSet(new RiakObjectId(bucketType, bucket, key), options);
        }

        public RiakDtSetResult DtFetchSet(RiakObjectId objectId, RiakDtFetchOptions options = null)
        {
            var message = new DtFetchReq {
                type = objectId.BucketType.ToRiakString(),
                bucket = objectId.Bucket.ToRiakString(),
                key = objectId.Key.ToRiakString()
            };

            options = options ?? new RiakDtFetchOptions();

            options.Populate(message);

            var result = UseConnection(conn => conn.PbcWriteRead<DtFetchReq, DtFetchResp>(message));

            if (!result.IsSuccess)
                return new RiakDtSetResult(RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline));

            var rsr =
                new RiakDtSetResult(RiakResult<RiakObject>.Success(new RiakObject(objectId, result.Value.value)));

            if (options.IncludeContext.HasValue && options.IncludeContext.Value)
                rsr.Context = result.Value.context;

            return rsr;
        }

        public RiakDtSetResult DtUpdateSet<T>(string bucketType,
                                              string bucket,
                                              string key,
                                              SerializeObjectToByteArray<T> serialize,
                                              byte[] context = null,
                                              List<T> adds = null,
                                              List<T> removes = null,
                                              RiakDtUpdateOptions options = null)
        {
            return DtUpdateSet(new RiakObjectId(bucketType, bucket, key), serialize, context, adds, removes, options);
        }

        public RiakDtSetResult DtUpdateSet<T>(RiakObjectId objectId,
                                              SerializeObjectToByteArray<T> serialize,
                                              byte[] context = null,
                                              List<T> adds = null,
                                              List<T> removes = null,
                                              RiakDtUpdateOptions options = null)
        {
            var request = new DtUpdateReq
            {
                type = objectId.BucketType.ToRiakString(),
                bucket = objectId.Bucket.ToRiakString(),
                key = objectId.Key.ToRiakString(),
                context = context
            };

            options = options ?? new RiakDtUpdateOptions();
            options.Populate(request);

            if (adds != null)
                request.op.set_op.adds.AddRange(adds.Select(a => serialize(a)));

            if (removes != null)
                request.op.set_op.removes.AddRange(removes.Select(r => serialize(r)));

            var response = UseConnection(conn => conn.PbcWriteRead<DtUpdateReq, DtUpdateResp>(request));

            var rdsr =
                new RiakDtSetResult(RiakResult<RiakObject>.Success(new RiakObject(objectId, response.Value.set_value)));

            if (options.IncludeContext)
                rdsr.Context = response.Value.context;

            if (options.ReturnBody)
                rdsr.Values = response.Value.set_value;

            return rdsr;
        }

        public RiakDtMapResult DtFetchMap(string bucket,
                                          string key,
                                          string bucketType = null,
                                          RiakDtFetchOptions options = null)
        {
            var message = new DtFetchReq
            {
                bucket = bucket.ToRiakString(),
                type = bucketType.ToRiakString(),
                key = key.ToRiakString()
            };

            options = options ?? new RiakDtFetchOptions();

            options.Populate(message);

            var result = UseConnection(conn => conn.PbcWriteRead<DtFetchReq, DtFetchResp>(message));

            if (!result.IsSuccess)
                return new RiakDtMapResult(RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline));

            var rmr =
                new RiakDtMapResult(RiakResult<RiakObject>.Success(new RiakObject(bucket, key, result.Value.value)));

            if (options.IncludeContext.HasValue && options.IncludeContext.Value)
                rmr.Context = result.Value.context;

            return rmr;
        }

        public RiakDtMapResult DtUpdateMap<T>(string bucketType,
                                              string bucket,
                                              string key,
                                              SerializeObjectToByteArray<T> serialize,
                                              byte[] context = null,
                                              List<RiakDtMapField> adds = null,
                                              List<RiakDtMapField> removes = null,
                                              /* Is this the right way to represent updates?
                                               * It seems like there should be something better, but it requires data
                                               * structures that track themselves and my guess is building the update
                                               * should be handled long before we get to calling DtUpdateMap<T>
                                               */
                                              List<MapUpdate> updates = null,
                                              RiakDtUpdateOptions options = null
            )
        {
            return DtUpdateMap(new RiakObjectId(bucketType, bucket, key),
                               serialize, context, adds, removes, updates, options);
        }

        public RiakDtMapResult DtUpdateMap<T>(RiakObjectId objectId,
                                              SerializeObjectToByteArray<T> serialize,
                                              byte[] context = null,
                                              List<RiakDtMapField> adds = null,
                                              List<RiakDtMapField> removes = null,
                                              List<MapUpdate> updates = null,
                                              RiakDtUpdateOptions options = null)
        {
            var request = new DtUpdateReq
            {
                type = objectId.BucketType.ToRiakString(),
                bucket = objectId.Bucket.ToRiakString(),
                key = objectId.Key.ToRiakString()
            };

            options = options ?? new RiakDtUpdateOptions();
            options.Populate(request);

            if (adds != null)
                request.op.map_op.adds.AddRange(adds.Select(a => a.ToMapField()));

            if (removes != null)
                request.op.map_op.removes.AddRange(removes.Select(a => a.ToMapField()));

            if (updates != null)
                request.op.map_op.updates.AddRange(updates);

            if (context != null)
                request.context = context;

            var result = UseConnection(conn => conn.PbcWriteRead<DtUpdateReq, DtUpdateResp>(request));

            if (!result.IsSuccess)
                return new RiakDtMapResult(RiakResult<RiakObject>.Error(result.ResultCode, result.ErrorMessage, result.NodeOffline));

            var rmr =
                new RiakDtMapResult(RiakResult<RiakObject>.Success(new RiakObject(objectId, result.Value.map_value)));

            if (options.IncludeContext)
                rmr.Context = result.Value.context;

            if (options.ReturnBody)
                rmr.Values.AddRange(result.Value.map_value.Select(mv => new RiakDtMapEntry(mv)));

            return rmr;
        }
    }
}