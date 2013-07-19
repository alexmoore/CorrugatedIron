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
using CorrugatedIron.Exceptions;
using CorrugatedIron.Extensions;
using CorrugatedIron.Messages;
using CorrugatedIron.Models.Rest;
using CorrugatedIron.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace CorrugatedIron.Comms
{
    public interface IRiakConnection : IDisposable
    {
        bool IsIdle { get; }

        void Disconnect();

        // PBC interface
        Task<RiakResult<TResult>> PbcRead<TResult>()
            where TResult : class, new();

        Task<RiakResult> PbcRead(MessageCode expectedMessageCode);

        Task<RiakResult> PbcWrite<TRequest>(TRequest request)
            where TRequest : class;

        Task<RiakResult> PbcWrite(MessageCode messageCode);

        Task<RiakResult<TResult>> PbcWriteRead<TRequest, TResult>(TRequest request)
            where TRequest : class
            where TResult : class, new();

        Task<RiakResult<TResult>> PbcWriteRead<TResult>(MessageCode messageCode)
            where TResult : class, new();

        Task<RiakResult> PbcWriteRead<TRequest>(TRequest request, MessageCode expectedMessageCode)
            where TRequest : class;

        Task<RiakResult> PbcWriteRead(MessageCode messageCode, MessageCode expectedMessageCode);

        Task<IEnumerable<RiakResult<TResult>>> PbcRepeatRead<TResult>(Func<RiakResult<TResult>, bool> repeatRead)
            where TResult : class, new();

        Task<RiakResult<IEnumerable<RiakResult<TResult>>>> PbcWriteRead<TResult>(MessageCode messageCode, Func<RiakResult<TResult>, bool> repeatRead)
            where TResult : class, new();

        Task<RiakResult<IEnumerable<RiakResult<TResult>>>> PbcWriteRead<TRequest, TResult>(TRequest request, Func<RiakResult<TResult>, bool> repeatRead)
            where TRequest : class
            where TResult : class, new();

        Task<RiakResult<IEnumerable<RiakResult<TResult>>>> PbcStreamRead<TResult>(Func<RiakResult<TResult>, bool> repeatRead, Action onFinish)
            where TResult : class, new();

        Task<RiakResult<IEnumerable<RiakResult<TResult>>>> PbcWriteStreamRead<TRequest, TResult>(TRequest request,
            Func<RiakResult<TResult>, bool> repeatRead, Action onFinish)
            where TRequest : class
            where TResult : class, new();

        Task<RiakResult<IEnumerable<RiakResult<TResult>>>> PbcWriteStreamRead<TResult>(MessageCode messageCode,
            Func<RiakResult<TResult>, bool> repeatRead, Action onFinish)
            where TResult : class, new();

        // REST interface
        Task<RiakResult<RiakRestResponse>> RestRequest(RiakRestRequest request);
    }

    internal class RiakConnection : IRiakConnection
    {
        private readonly string _restRootUrl;
        private readonly RiakPbcSocket _socket;

        public bool IsIdle
        {
            get { return _socket.IsConnected; }
        }

        static RiakConnection()
        {
            ServicePointManager.ServerCertificateValidationCallback += ServerValidationCallback;
        }

        public RiakConnection(IRiakNodeConfiguration nodeConfiguration)
        {
            _restRootUrl = @"{0}://{1}:{2}".Fmt(nodeConfiguration.RestScheme, nodeConfiguration.HostAddress, nodeConfiguration.RestPort);
            _socket = new RiakPbcSocket(nodeConfiguration.HostAddress, nodeConfiguration.PbcPort, nodeConfiguration.NetworkReadTimeout,
                nodeConfiguration.NetworkWriteTimeout);
        }

        public Task<RiakResult<TResult>> PbcRead<TResult>()
            where TResult : class, new()
        {
            return _socket.Read<TResult>()
                .ContinueWith(t => RiakResult<TResult>.Success(t.Result));
        }

        public Task<RiakResult> PbcRead(MessageCode expectedMessageCode)
        {
            return _socket.Read(expectedMessageCode)
                .ContinueWith(t => RiakResult.Success());
        }

        public Task<IEnumerable<RiakResult<TResult>>> PbcRepeatRead<TResult>(Func<RiakResult<TResult>, bool> repeatRead)
            where TResult : class, new()
        {
            var results = new List<RiakResult<TResult>>();
            Func<Task<TResult>, IEnumerable<RiakResult<TResult>>> continuation = null;

            continuation = t =>
                {
                    if (!t.IsFaulted)
                    {
                        var riakResult = RiakResult<TResult>.Success(t.Result);
                        results.Add(riakResult);

                        if (repeatRead(riakResult))
                        {
                            return _socket.Read<TResult>().ContinueWith(continuation).Result;
                        }

                        return results;
                    }
                    results.Add(RiakResult<TResult>.Error(ResultCode.CommunicationError, "Not sure what goes here.", false));

                    return results;
                };

            return _socket.Read<TResult>().ContinueWith(continuation);
        }

        public Task<RiakResult> PbcWrite<TRequest>(TRequest request)
            where TRequest : class
        {
            return _socket.Write(request)
                .ContinueWith(t => RiakResult.Success());
        }

        public Task<RiakResult> PbcWrite(MessageCode messageCode)
        {
            return _socket.Write(messageCode)
                .ContinueWith(t => RiakResult.Success());
        }

        public Task<RiakResult<TResult>> PbcWriteRead<TRequest, TResult>(TRequest request)
            where TRequest : class
            where TResult : class, new()
        {
            return PbcWrite(request)
                .ContinueWith(t =>
                    {
                        if (t.Result.IsSuccess)
                        {
                            return PbcRead<TResult>().Result;
                        }

                        return RiakResult<TResult>.Error(t.Result.ResultCode, t.Result.ErrorMessage, t.Result.NodeOffline);
                    });
        }

        public Task<RiakResult> PbcWriteRead<TRequest>(TRequest request, MessageCode expectedMessageCode)
            where TRequest : class
        {
            return PbcWrite(request)
                .ContinueWith(t =>
                    {
                        if (t.Result.IsSuccess)
                        {
                            return PbcRead(expectedMessageCode).Result;
                        }
                        return RiakResult.Error(t.Result.ResultCode, t.Result.ErrorMessage, t.Result.NodeOffline);
                    });
        }

        public Task<RiakResult<TResult>> PbcWriteRead<TResult>(MessageCode messageCode)
            where TResult : class, new()
        {
            return PbcWrite(messageCode)
                .ContinueWith(t =>
                    {
                        if (t.Result.IsSuccess)
                        {
                            return PbcRead<TResult>().Result;
                        }
                        return RiakResult<TResult>.Error(t.Result.ResultCode, t.Result.ErrorMessage, t.Result.NodeOffline);
                    });
        }

        public Task<RiakResult> PbcWriteRead(MessageCode messageCode, MessageCode expectedMessageCode)
        {
            return PbcWrite(messageCode)
                .ContinueWith(t =>
                    {
                        if (t.Result.IsSuccess)
                        {
                            return PbcRead(expectedMessageCode).Result;
                        }
                        return RiakResult.Error(t.Result.ResultCode, t.Result.ErrorMessage, t.Result.NodeOffline);
                    });
        }

        public Task<RiakResult<IEnumerable<RiakResult<TResult>>>> PbcWriteRead<TRequest, TResult>(TRequest request,
            Func<RiakResult<TResult>, bool> repeatRead)
            where TRequest : class
            where TResult : class, new()
        {
            return PbcWrite(request)
                .ContinueWith(wt =>
                    {
                        if (wt.Result.IsSuccess)
                        {
                            var result = PbcRepeatRead(repeatRead).Result;
                            return RiakResult<IEnumerable<RiakResult<TResult>>>.Success(result);
                        }
                        return RiakResult<IEnumerable<RiakResult<TResult>>>.Error(wt.Result.ResultCode, wt.Result.ErrorMessage, wt.Result.NodeOffline);
                    });
        }

        public Task<RiakResult<IEnumerable<RiakResult<TResult>>>> PbcWriteRead<TResult>(MessageCode messageCode,
            Func<RiakResult<TResult>, bool> repeatRead)
            where TResult : class, new()
        {
            return PbcWrite(messageCode)
                .ContinueWith(wt =>
                    {
                        if (wt.Result.IsSuccess)
                        {
                            var result = PbcRepeatRead(repeatRead).Result;
                            return RiakResult<IEnumerable<RiakResult<TResult>>>.Success(result);
                        }
                        return RiakResult<IEnumerable<RiakResult<TResult>>>.Error(wt.Result.ResultCode, wt.Result.ErrorMessage, wt.Result.NodeOffline);
                    });
        }

        public Task<RiakResult<IEnumerable<RiakResult<TResult>>>> PbcStreamRead<TResult>(Func<RiakResult<TResult>, bool> repeatRead, Action onFinish)
            where TResult : class, new()
        {
            //var streamer = PbcStreamReadIterator(repeatRead, onFinish);
            //return RiakResult<IEnumerable<RiakResult<TResult>>>.Success(streamer);
            throw new NotImplementedException();
        }

        private IEnumerable<RiakResult<TResult>> PbcStreamReadIterator<TResult>(Func<RiakResult<TResult>, bool> repeatRead, Action onFinish)
            where TResult : class, new()
        {
            //RiakResult<TResult> result;

            //do
            //{
            //    result = PbcRead<TResult>();
            //    if(!result.IsSuccess) break;
            //    yield return result;
            //} while(repeatRead(result));

            //// clean up first..
            //onFinish();

            //// then return the failure to the client to indicate failure
            //yield return result;
            throw new NotImplementedException();
        }

        public Task<RiakResult<IEnumerable<RiakResult<TResult>>>> PbcWriteStreamRead<TRequest, TResult>(TRequest request,
            Func<RiakResult<TResult>, bool> repeatRead, Action onFinish)
            where TRequest : class
            where TResult : class, new()
        {
            //var streamer = PbcWriteStreamReadIterator(request, repeatRead, onFinish);
            //return RiakResult<IEnumerable<RiakResult<TResult>>>.Success(streamer);
            throw new NotImplementedException();
        }

        public Task<RiakResult<IEnumerable<RiakResult<TResult>>>> PbcWriteStreamRead<TResult>(MessageCode messageCode,
            Func<RiakResult<TResult>, bool> repeatRead, Action onFinish)
            where TResult : class, new()
        {
            //var streamer = PbcWriteStreamReadIterator(messageCode, repeatRead, onFinish);
            //return RiakResult<IEnumerable<RiakResult<TResult>>>.Success(streamer);
            throw new NotImplementedException();
        }

        private IEnumerable<RiakResult<TResult>> PbcWriteStreamReadIterator<TRequest, TResult>(TRequest request,
            Func<RiakResult<TResult>, bool> repeatRead, Action onFinish)
            where TRequest : class
            where TResult : class, new()
        {
            //var writeResult = PbcWrite(request);
            //if(writeResult.IsSuccess)
            //{
            //    return PbcStreamReadIterator(repeatRead, onFinish);
            //}
            //onFinish();
            //return new[] { RiakResult<TResult>.Error(writeResult.ResultCode, writeResult.ErrorMessage, writeResult.NodeOffline) };
            throw new NotImplementedException();
        }

        private IEnumerable<RiakResult<TResult>> PbcWriteStreamReadIterator<TResult>(MessageCode messageCode,
            Func<RiakResult<TResult>, bool> repeatRead, Action onFinish)
            where TResult : class, new()
        {
            //var writeResult = PbcWrite(messageCode);
            //if(writeResult.IsSuccess)
            //{
            //    return PbcStreamReadIterator(repeatRead, onFinish);
            //}
            //onFinish();
            //return new[] { RiakResult<TResult>.Error(writeResult.ResultCode, writeResult.ErrorMessage, writeResult.NodeOffline) };
            throw new NotImplementedException();
        }

        public Task<RiakResult<RiakRestResponse>> RestRequest(RiakRestRequest request)
        {
            //var baseUri = new StringBuilder(_restRootUrl).Append(request.Uri);
            //if(request.QueryParams.Count > 0)
            //{
            //    baseUri.Append("?");
            //    var first = request.QueryParams.First();
            //    baseUri.Append(first.Key.UrlEncoded()).Append("=").Append(first.Value.UrlEncoded());
            //    request.QueryParams.Skip(1).ForEach(kv => baseUri.Append("&").Append(kv.Key.UrlEncoded()).Append("=").Append(kv.Value.UrlEncoded()));
            //}
            //var targetUri = new Uri(baseUri.ToString());

            //var req = (HttpWebRequest)WebRequest.Create(targetUri);
            //req.KeepAlive = true;
            //req.Method = request.Method;
            //req.Credentials = CredentialCache.DefaultCredentials;

            //if(!string.IsNullOrWhiteSpace(request.ContentType))
            //{
            //    req.ContentType = request.ContentType;
            //}

            //if(!request.Cache)
            //{
            //    req.Headers.Set(RiakConstants.Rest.HttpHeaders.DisableCacheKey, RiakConstants.Rest.HttpHeaders.DisableCacheValue);
            //}

            //request.Headers.ForEach(h => req.Headers.Set(h.Key, h.Value));

            //if(request.Body != null && request.Body.Length > 0)
            //{
            //    req.ContentLength = request.Body.Length;
            //    using(var writer = req.GetRequestStream())
            //    {
            //        writer.Write(request.Body, 0, request.Body.Length);
            //    }
            //}
            //else
            //{
            //    req.ContentLength = 0;
            //}

            //try
            //{
            //    var response = (HttpWebResponse)req.GetResponse();

            //    var result = new RiakRestResponse
            //    {
            //        ContentLength = response.ContentLength,
            //        ContentType = response.ContentType,
            //        StatusCode = response.StatusCode,
            //        Headers = response.Headers.AllKeys.ToDictionary(k => k, k => response.Headers[k]),
            //        ContentEncoding = !string.IsNullOrWhiteSpace(response.ContentEncoding)
            //            ? Encoding.GetEncoding(response.ContentEncoding)
            //            : Encoding.Default
            //    };

            //    if (response.ContentLength > 0)
            //    {
            //        using (var responseStream = response.GetResponseStream())
            //        {
            //            if (responseStream != null)
            //            {
            //                using (var reader = new StreamReader(responseStream, result.ContentEncoding))
            //                {
            //                    result.Body = reader.ReadToEnd();
            //                }
            //            }
            //        }
            //    }

            //    return RiakResult<RiakRestResponse>.Success(result);
            //}
            //catch (RiakException ex)
            //{
            //    return RiakResult<RiakRestResponse>.Error(ResultCode.CommunicationError, ex.Message, ex.NodeOffline);
            //}
            //catch (WebException ex)
            //{
            //    if (ex.Status == WebExceptionStatus.ProtocolError)
            //    {
            //        return RiakResult<RiakRestResponse>.Error(ResultCode.HttpError, ex.Message, false);
            //    }

            //    return RiakResult<RiakRestResponse>.Error(ResultCode.HttpError, ex.Message, true);
            //}
            //catch (Exception ex)
            //{
            //    return RiakResult<RiakRestResponse>.Error(ResultCode.CommunicationError, ex.Message, true);
            //}
            throw new NotImplementedException();
        }

        private static bool ServerValidationCallback(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            return true;
        }

        public void Dispose()
        {
            _socket.Dispose();
            Disconnect();
        }

        public void Disconnect()
        {
            _socket.Disconnect();
        }
    }
}
