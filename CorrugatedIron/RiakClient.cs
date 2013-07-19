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

using System.Numerics;
using CorrugatedIron.Models;
using CorrugatedIron.Models.Index;
using CorrugatedIron.Models.MapReduce;
using CorrugatedIron.Models.Search;
using CorrugatedIron.Util;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CorrugatedIron
{
    public interface IRiakClient
    {
        RiakResult Ping();

        RiakResult<RiakObject> Get(string bucket, string key, RiakGetOptions options = null);
        RiakResult<RiakObject> Get(RiakObjectId objectId, RiakGetOptions options = null);
        IEnumerable<RiakResult<RiakObject>> Get(IEnumerable<RiakObjectId> bucketKeyPairs, RiakGetOptions options = null);

        RiakCounterResult IncrementCounter(string bucket, string counter, long amount, RiakCounterUpdateOptions options = null);
        RiakCounterResult GetCounter(string bucket, string counter, RiakCounterGetOptions options = null);

        RiakResult<RiakObject> Put(RiakObject value, RiakPutOptions options = null);
        IEnumerable<RiakResult<RiakObject>> Put(IEnumerable<RiakObject> values, RiakPutOptions options = null);

        RiakResult Delete(string bucket, string key, RiakDeleteOptions options = null);
        RiakResult Delete(RiakObjectId objectId, RiakDeleteOptions options = null);
        IEnumerable<RiakResult> Delete(IEnumerable<RiakObjectId> objectIds, RiakDeleteOptions options = null);
        IEnumerable<RiakResult> DeleteBucket(string bucket, uint rwVal = RiakConstants.Defaults.RVal);

        RiakResult<RiakSearchResult> Search(RiakSearchRequest search);

        RiakResult<RiakMapReduceResult> MapReduce(RiakMapReduceQuery query);
        RiakResult<RiakStreamedMapReduceResult> StreamMapReduce(RiakMapReduceQuery query);

        RiakResult<IEnumerable<string>> ListBuckets();
        RiakResult<IEnumerable<string>> StreamListBuckets();
        RiakResult<IEnumerable<string>> ListKeys(string bucket);
        RiakResult<IEnumerable<string>> StreamListKeys(string bucket);

        RiakResult<RiakBucketProperties> GetBucketProperties(string bucket);
        RiakResult SetBucketProperties(string bucket, RiakBucketProperties properties, bool useHttp = false);
        RiakResult ResetBucketProperties(string bucket, bool useHttp = false);

        RiakResult<IList<RiakObject>> WalkLinks(RiakObject riakObject, IList<RiakLink> riakLinks);

        RiakResult<RiakServerInfo> GetServerInfo();

        RiakResult<RiakIndexResult> IndexGet(string bucket, string indexName, int value, RiakIndexGetOptions options = null);
        RiakResult<RiakIndexResult> IndexGet(string bucket, string indexName, string value, RiakIndexGetOptions options = null);
        RiakResult<RiakIndexResult> IndexGet(string bucket, string indexName, int minValue, int maxValue, RiakIndexGetOptions options = null);
        RiakResult<RiakIndexResult> IndexGet(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null);

        RiakResult<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, BigInteger value, RiakIndexGetOptions options = null);
        RiakResult<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, string value, RiakIndexGetOptions options = null);
        RiakResult<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, BigInteger minValue, BigInteger maxValue, RiakIndexGetOptions options = null);
        RiakResult<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null);

        RiakResult<IList<string>> ListKeysFromIndex(string bucket);

        Task Batch(Action<IRiakBatchClient> batchAction);
    }

    public class RiakClient : IRiakClient
    {
        private readonly IRiakAsyncClient _client;

        public RiakClient(IRiakAsyncClient client)
        {
            _client = client;
        }

        public RiakResult Ping()
        {
            return _client.Ping().Result;
        }

        public RiakResult<RiakObject> Get(string bucket, string key, RiakGetOptions options = null)
        {
            return _client.Get(bucket, key, options).Result;
        }

        public RiakResult<RiakObject> Get(RiakObjectId objectId, RiakGetOptions options = null)
        {
            return _client.Get(objectId, options).Result;
        }

        public IEnumerable<RiakResult<RiakObject>> Get(IEnumerable<RiakObjectId> bucketKeyPairs, RiakGetOptions options = null)
        {
            return _client.Get(bucketKeyPairs, options).Result;
        }

        public RiakCounterResult IncrementCounter(string bucket, string counter, long amount, RiakCounterUpdateOptions options = null)
        {
            return _client.IncrementCounter(bucket, counter, amount, options).Result;
        }

        public RiakCounterResult GetCounter(string bucket, string counter, RiakCounterGetOptions options = null) 
        {
            return _client.GetCounter(bucket, counter, options).Result;
        }

        public IEnumerable<RiakResult<RiakObject>> Put(IEnumerable<RiakObject> values, RiakPutOptions options)
        {
            return _client.Put(values, options).Result;
        }

        public RiakResult<RiakObject> Put(RiakObject value, RiakPutOptions options)
        {
            return _client.Put(value, options).Result;
        }

        public RiakResult Delete(string bucket, string key, RiakDeleteOptions options = null)
        {
            return _client.Delete(bucket, key, options).Result;
        }

        public RiakResult Delete(RiakObjectId objectId, RiakDeleteOptions options = null)
        {
            return _client.Delete(objectId.Bucket, objectId.Key, options).Result;
        }

        public IEnumerable<RiakResult> Delete(IEnumerable<RiakObjectId> objectIds, RiakDeleteOptions options = null)
        {
            return _client.Delete(objectIds, options).Result;
        }

        public IEnumerable<RiakResult> DeleteBucket(string bucket, uint rwVal = RiakConstants.Defaults.RVal)
        {
            return _client.DeleteBucket(bucket, rwVal).Result;
        }

        public RiakResult<RiakSearchResult> Search(RiakSearchRequest search)
        {
            return _client.Search(search).Result;
        }

        public RiakResult<RiakMapReduceResult> MapReduce(RiakMapReduceQuery query)
        {
            return _client.MapReduce(query).Result;
        }

        public RiakResult<RiakStreamedMapReduceResult> StreamMapReduce(RiakMapReduceQuery query)
        {
            return _client.StreamMapReduce(query).Result;
        }

        public RiakResult<IEnumerable<string>> StreamListBuckets()
        {
            return _client.StreamListBuckets().Result;
        }

        public RiakResult<IEnumerable<string>> ListBuckets()
        {
            return _client.ListBuckets().Result;
        }

        public RiakResult<IEnumerable<string>> ListKeys(string bucket)
        {
            return _client.ListKeys(bucket).Result;
        }

        public RiakResult<IEnumerable<string>> StreamListKeys(string bucket)
        {
            return _client.StreamListKeys(bucket).Result;
        }

        public RiakResult<RiakBucketProperties> GetBucketProperties(string bucket)
        {
            return _client.GetBucketProperties(bucket).Result;
        }

        public RiakResult SetBucketProperties(string bucket, RiakBucketProperties properties, bool useHttp = false)
        {
            return _client.SetBucketProperties(bucket, properties, useHttp).Result;
        }

        public RiakResult ResetBucketProperties(string bucket, bool useHttp = false)
        {
            return _client.ResetBucketProperties(bucket, useHttp).Result;
        }

        public RiakResult<RiakIndexResult> IndexGet(string bucket, string indexName, int value, RiakIndexGetOptions options = null)
        {
            return _client.IndexGet(bucket, indexName, value, options).Result;
        }

        public RiakResult<RiakIndexResult> IndexGet(string bucket, string indexName, string value, RiakIndexGetOptions options = null)
        {
            return _client.IndexGet(bucket, indexName, value, options).Result;
        }

        public RiakResult<RiakIndexResult> IndexGet(string bucket, string indexName, int minValue, int maxValue, RiakIndexGetOptions options = null)
        {
            return _client.IndexGet(bucket, indexName, minValue, maxValue, options).Result;
        }

        public RiakResult<RiakIndexResult> IndexGet(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null)
        {
            return _client.IndexGet(bucket, indexName, minValue, maxValue, options).Result;
        }

        public RiakResult<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, BigInteger value, RiakIndexGetOptions options = null)
        {
            return _client.StreamIndexGet(bucket, indexName, value, options).Result;
        }

        public RiakResult<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, string value, RiakIndexGetOptions options = null)
        {
            return _client.StreamIndexGet(bucket, indexName, value, options).Result;
        }

        public RiakResult<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, BigInteger minValue, BigInteger maxValue, RiakIndexGetOptions options = null)
        {
            return _client.StreamIndexGet(bucket, indexName, minValue, maxValue, options).Result;
        }

        public RiakResult<RiakStreamedIndexResult> StreamIndexGet(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null)
        {
            return _client.StreamIndexGet(bucket, indexName, minValue, maxValue, options).Result;
        }

        public RiakResult<IList<string>> ListKeysFromIndex(string bucket)
        {
            return _client.ListKeysFromIndex(bucket).Result;
        }

        public RiakResult<IList<RiakObject>> WalkLinks(RiakObject riakObject, IList<RiakLink> riakLinks)
        {
            return _client.WalkLinks(riakObject, riakLinks).Result;
        }

        public RiakResult<RiakServerInfo> GetServerInfo()
        {
            return _client.GetServerInfo().Result;
        }

        public Task Batch(Action<IRiakBatchClient> batchAction)
        {
            //return _client.Batch(batchAction).Result;
            throw new NotImplementedException();
        }

        public void Batch<T>(Func<IRiakBatchClient, T> batchFunc)
        {
            //return () => _client.Batch(batchFunc).Result;
            throw new NotImplementedException();
        }
    }
}