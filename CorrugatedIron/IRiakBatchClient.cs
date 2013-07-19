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
using CorrugatedIron.Models;
using CorrugatedIron.Models.Index;
using CorrugatedIron.Models.MapReduce;
using CorrugatedIron.Models.Search;
using CorrugatedIron.Util;
using System.Collections.Generic;
using System.Numerics;

namespace CorrugatedIron
{
    public interface IRiakBatchClient
    {
        int RetryCount { get; set; }
        Task<RiakResult> Ping();

        Task<RiakResult<RiakObject>> Get(RiakObjectId objectId, RiakGetOptions options = null);
        Task<RiakResult<RiakObject>> Get(string bucket, string key, RiakGetOptions options = null);
        Task<IEnumerable<RiakResult<RiakObject>>> Get(IEnumerable<RiakObjectId> bucketKeyPairs, RiakGetOptions options = null);

        Task<RiakCounterResult> IncrementCounter(string bucket, string counter, long amount, RiakCounterUpdateOptions options = null);
        Task<RiakCounterResult> GetCounter(string bucket, string counter, RiakCounterGetOptions options = null);

        Task<RiakResult<RiakObject>> Put(RiakObject value, RiakPutOptions options = null);
        Task<IEnumerable<RiakResult<RiakObject>>> Put(IEnumerable<RiakObject> values, RiakPutOptions options = null);

        Task<RiakResult> Delete(string bucket, string key, RiakDeleteOptions options = null);
        Task<RiakResult> Delete(RiakObjectId objectId, RiakDeleteOptions options = null);
        Task<IEnumerable<RiakResult>> Delete(IEnumerable<RiakObjectId> objectIds, RiakDeleteOptions options = null);
        Task<IEnumerable<RiakResult>> DeleteBucket(string bucket, uint rwVal = RiakConstants.Defaults.RVal);

        Task<RiakResult<RiakSearchResult>> Search(RiakSearchRequest search);

        Task<RiakResult<RiakMapReduceResult>> MapReduce(RiakMapReduceQuery query);
        Task<RiakResult<RiakStreamedMapReduceResult>> StreamMapReduce(RiakMapReduceQuery query);

        Task<RiakResult<IEnumerable<string>>> ListBuckets();
        Task<RiakResult<IEnumerable<string>>> StreamListBuckets();
        Task<RiakResult<IEnumerable<string>>> ListKeys(string bucket);
        Task<RiakResult<IEnumerable<string>>> StreamListKeys(string bucket);

        Task<RiakResult<RiakBucketProperties>> GetBucketProperties(string bucket);
        Task<RiakResult> SetBucketProperties(string bucket, RiakBucketProperties properties, bool useHttp = false);
        Task<RiakResult> ResetBucketProperties(string bucket, bool useHttp = false);

        Task<RiakResult<IList<RiakObject>>> WalkLinks(RiakObject riakObject, IList<RiakLink> riakLinks);

        Task<RiakResult<RiakServerInfo>> GetServerInfo();

        Task<RiakResult<RiakIndexResult>> IndexGet(string bucket, string indexName, BigInteger value, RiakIndexGetOptions options = null);
        Task<RiakResult<RiakIndexResult>> IndexGet(string bucket, string indexName, string value, RiakIndexGetOptions options = null);
        Task<RiakResult<RiakIndexResult>> IndexGet(string bucket, string indexName, BigInteger minValue, BigInteger maxValue, RiakIndexGetOptions options = null);
        Task<RiakResult<RiakIndexResult>> IndexGet(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null);

        Task<RiakResult<RiakStreamedIndexResult>> StreamIndexGet(string bucket, string indexName, BigInteger value, RiakIndexGetOptions options = null);
        Task<RiakResult<RiakStreamedIndexResult>> StreamIndexGet(string bucket, string indexName, string value, RiakIndexGetOptions options = null);
        Task<RiakResult<RiakStreamedIndexResult>> StreamIndexGet(string bucket, string indexName, BigInteger minValue, BigInteger maxValue, RiakIndexGetOptions options = null);
        Task<RiakResult<RiakStreamedIndexResult>> StreamIndexGet(string bucket, string indexName, string minValue, string maxValue, RiakIndexGetOptions options = null);

        Task<RiakResult<IList<string>>> ListKeysFromIndex(string bucket);

        //RiakResult<RiakSearchResult> Search(Action<RiakSearchRequest> prepareRequest)
    }

}