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
using CorrugatedIron.Exceptions;
using CorrugatedIron.Extensions;
using CorrugatedIron.Messages;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;

namespace CorrugatedIron.Comms
{
    internal class RiakPbcSocket : IDisposable
    {
        // Size of the buffer to use for network operations.
        private const int NetworkBufferSize = 4096;
        private const int PbcHeaderSize = sizeof(int) + sizeof(byte);


        private static readonly Dictionary<MessageCode, Type> MessageCodeToTypeMap;
        private static readonly Dictionary<Type, MessageCode> TypeToMessageCodeMap;

        private readonly string _server;
        private readonly int _port;
        private readonly int _receiveTimeout;
        private readonly int _sendTimeout;
        private readonly SocketAsyncEventArgs _saea;
        private readonly byte[] _networkBuffer;

        private Socket _pbcSocket;

        public bool IsConnected
        {
            get { return _pbcSocket != null && _pbcSocket.Connected; }
        }

        static RiakPbcSocket()
        {
            MessageCodeToTypeMap = new Dictionary<MessageCode, Type>
            {
                { MessageCode.ErrorResp, typeof(RpbErrorResp) },
                { MessageCode.GetClientIdResp, typeof(RpbGetClientIdResp) },
                { MessageCode.SetClientIdReq, typeof(RpbSetClientIdReq) },
                { MessageCode.GetServerInfoResp, typeof(RpbGetServerInfoResp) },
                { MessageCode.GetReq, typeof(RpbGetReq) },
                { MessageCode.GetResp, typeof(RpbGetResp) },
                { MessageCode.PutReq, typeof(RpbPutReq) },
                { MessageCode.PutResp, typeof(RpbPutResp) },
                { MessageCode.DelReq, typeof(RpbDelReq) },
                { MessageCode.ListBucketsReq, typeof(RpbListBucketsReq) },
                { MessageCode.ListBucketsResp, typeof(RpbListBucketsResp) },
                { MessageCode.ListKeysReq, typeof(RpbListKeysReq) },
                { MessageCode.ListKeysResp, typeof(RpbListKeysResp) },
                { MessageCode.GetBucketReq, typeof(RpbGetBucketReq) },
                { MessageCode.GetBucketResp, typeof(RpbGetBucketResp) },
                { MessageCode.SetBucketReq, typeof(RpbSetBucketReq) },
                { MessageCode.MapRedReq, typeof(RpbMapRedReq) },
                { MessageCode.MapRedResp, typeof(RpbMapRedResp) },
                { MessageCode.IndexReq, typeof(RpbIndexReq) },
                { MessageCode.IndexResp, typeof(RpbIndexResp) },
                { MessageCode.SearchQueryReq, typeof(RpbSearchQueryReq) },
                { MessageCode.SearchQueryResp, typeof(RpbSearchQueryResp) },
                { MessageCode.ResetBucketReq, typeof(RpbResetBucketReq) },
                { MessageCode.CsBucketReq, typeof(RpbCSBucketReq) },
                { MessageCode.CsBucketResp, typeof(RpbCSBucketResp) },
                { MessageCode.CounterUpdateReq, typeof(RpbCounterUpdateReq) },
                { MessageCode.CounterUpdateResp, typeof(RpbCounterUpdateResp) },
                { MessageCode.CounterGetReq, typeof(RpbCounterGetReq) },
                { MessageCode.CounterGetResp, typeof(RpbCounterGetResp) }
            };

            TypeToMessageCodeMap = new Dictionary<Type, MessageCode>();

            foreach(var item in MessageCodeToTypeMap)
            {
                TypeToMessageCodeMap.Add(item.Value, item.Key);
            }
        }

        // indicates that the socket has read data into its buffer, but hasn't
        // yet passed it on to the caller.
        private bool _hasData;

        // A count of how many bytes there are left to read for this operation
        // to be finished.
        private int _bytesLeft;

        // Offset into the source buffer to use for the next operation.
        private int _sourceOffset;

        // Buffer to use for network operations.
        private byte[] _buffer;

        // Offset into the _buffer to use for the next operation.
        private int _bufferOffset;

        // Result of the last socket operation.
        private SocketError _sockError;

        // tells us when the job has been finished.
        private readonly AutoResetEvent _signal;

        public RiakPbcSocket(string server, int port, int receiveTimeout, int sendTimeout)
        {
            _server = server;
            _port = port;
            _receiveTimeout = receiveTimeout;
            _sendTimeout = sendTimeout;

            _signal = new AutoResetEvent(false);

            var host = Dns.GetHostEntry(server);
            var hostEndPoint = new IPEndPoint(host.AddressList[host.AddressList.Length - 1], port);

            _networkBuffer = new byte[NetworkBufferSize];

            _saea = new SocketAsyncEventArgs
            {
                RemoteEndPoint = hostEndPoint
            };
            _saea.SetBuffer(_networkBuffer, 0, 0);
            _saea.Completed += (s, e) => HandleCompleted();
        }

        private Task<int> Write(byte[] data, int bytesToWrite)
        {
            _buffer = data;
            _bytesLeft = bytesToWrite;
            _bufferOffset = 0;

            return Task.Factory.StartNew(() =>
                {
                    HandleWrite(HandleCompleted);
                    _signal.WaitOne();
                    return bytesToWrite - _bytesLeft;
                });
        }

        private Task<Tuple<byte[], int>> Read(byte[] data, int bytesToRead)
        {
            _buffer = data;
            _bytesLeft = bytesToRead;
            _bufferOffset = 0;

            return Task.Factory.StartNew(() =>
                {
                    HandleRead(HandleCompleted);
                    _signal.WaitOne();
                    return Tuple.Create(data, bytesToRead - _bytesLeft);
                });
        }

        private void HandleRead(Action completed)
        {
            while (true)
            {
                if (_bytesLeft == 0)
                {
                    Finish();
                    break;
                }

                if (!_hasData)
                {
                    if (!_pbcSocket.ReceiveAsync(_saea))
                    {
                        completed();
                    }
                    break;
                }

                var diff = _saea.BytesTransferred - _sourceOffset;
                var actualByteCount = Math.Min(_bytesLeft, diff);
                Buffer.BlockCopy(_saea.Buffer, _saea.Offset + _sourceOffset, _buffer, _bufferOffset, actualByteCount);

                _hasData = _bytesLeft < diff;

                _sourceOffset += actualByteCount;
                _bytesLeft -= actualByteCount;
                _bufferOffset += actualByteCount;
            }
        }

        private void HandleWrite(Action completed)
        {
            if (_bytesLeft == 0)
            {
                Finish();
            }
            else
            {
                var actualByteCount = Math.Min(_bytesLeft, _networkBuffer.Length);
                Buffer.BlockCopy(_buffer, _bufferOffset, _saea.Buffer, _saea.Offset, actualByteCount);
                _saea.SetBuffer(_saea.Offset, actualByteCount);

                if (!_pbcSocket.SendAsync(_saea))
                {
                    completed();
                }
            }
        }

        private void Finish()
        {
            _buffer = null;
            _signal.Set();
        }

        private void HandleCompleted()
        {
            _sockError = _saea.SocketError;

            if (_saea.LastOperation == SocketAsyncOperation.Receive)
            {
                _hasData = _sockError == SocketError.Success && _saea.BytesTransferred > 0;
                _sourceOffset = 0;
                HandleRead(HandleCompleted);
            }
            else if (_saea.LastOperation == SocketAsyncOperation.Send)
            {
                _bufferOffset += _saea.BytesTransferred;
                _bytesLeft -= _saea.BytesTransferred;
                HandleWrite(HandleCompleted);
            }
            else if (_saea.LastOperation == SocketAsyncOperation.Connect)
            {
                _hasData = false;
                _bytesLeft = 0;
                _sourceOffset = 0;
                _bufferOffset = 0;

                _pbcSocket = _saea.ConnectSocket;
                _pbcSocket.ReceiveTimeout = _receiveTimeout;
                _pbcSocket.SendTimeout = _sendTimeout;
                _signal.Set();
            }
        }

        private static Tuple<MessageCode, int> ReadHeader(byte[] buffer)
        {
            var code = (MessageCode)buffer[4];
            int size = (buffer[0] << 24) | (buffer[1] << 16) | (buffer[2] << 8) | buffer[3];
            return Tuple.Create(code, size);
        }

        private static void WriteHeader(MessageCode code, int payloadSize, byte[] buffer)
        {
            var actualSize = payloadSize + 1;
            buffer[0] = (byte)(actualSize >> 24);
            buffer[1] = (byte)(actualSize >> 16);
            buffer[2] = (byte)(actualSize >> 8);
            buffer[3] = (byte)(actualSize);
            buffer[4] = (byte)(code);
        }

        private byte[] CreatePayload(MessageCode code, int payloadSize)
        {
            var payload = new byte[payloadSize + PbcHeaderSize];
            WriteHeader(code, payloadSize, payload);
            return payload;
        }

        public Task Write(MessageCode messageCode)
        {
            var messageBody = CreatePayload(messageCode, 0);

            return Connect()
                .ContinueWith(t => Write(messageBody, messageBody.Length).Result)
                .ContinueWith(t =>
                {
                    if (t.Result == 0)
                    {
                        throw new RiakException("Failed to send data to server - Timed Out: {0}:{1}".Fmt(_server, _port));
                    }
                });
        }

        public Task Write<T>(T message) where T : class
        {
            byte[] messageBody;
            var messageLength = 0L;
            var messageCode = TypeToMessageCodeMap[typeof(T)];

            using(var memStream = new MemoryStream())
            {
                // add a buffer to the start of the array to put the size and message code
                memStream.Position += PbcHeaderSize;
                Serializer.Serialize(memStream, message);
                messageBody = memStream.GetBuffer();
                messageLength = memStream.Position;
            }

            // check to make sure something was written, otherwise we'll have to create a new array
            if (messageLength == PbcHeaderSize)
            {
                messageBody = CreatePayload(messageCode, 0);
            }
            else
            {
                WriteHeader(messageCode, (int)messageLength - PbcHeaderSize, messageBody);
            }

            return Connect()
                .ContinueWith(t => Write(messageBody, (int)messageLength).Result);
        }

        public Task<MessageCode> Read(MessageCode expectedCode)
        {
            return Connect()
                .ContinueWith(t => Read(new byte[PbcHeaderSize], PbcHeaderSize).Result)
                .ContinueWith(t =>
                {
                    var bytesRead = t.Result.Item2;

                    if (bytesRead == 0)
                    {
                        throw new RiakException("Failed to receive data from server - Timed Out: {0}:{1}".Fmt(_server, _port));
                    }

                    var header = t.Result.Item1;
                    var parsedHeader = ReadHeader(header);
                    return parsedHeader.Item1;
                });
        }

        public Task<T> Read<T>() where T : new()
        {
            return Connect()
                .ContinueWith(t => Read(new byte[PbcHeaderSize], PbcHeaderSize).Result)
                .ContinueWith(t =>
                {
                    var bytesRead = t.Result.Item2;

                    if (bytesRead == 0)
                    {
                        throw new RiakException("Failed to receive data from server - Timed Out: {0}:{1}".Fmt(_server, _port));
                    }

                    var header = t.Result.Item1;
                    var parsedHeader = ReadHeader(header);
                    var code = parsedHeader.Item1;
                    var size = parsedHeader.Item2;

                    if (code == MessageCode.ErrorResp)
                    {
                        var error = DeserializeInstance<RpbErrorResp>(size).Result;
                        throw new RiakException(error.errcode, error.errmsg.FromRiakString());
                    }

                    if(!MessageCodeToTypeMap.ContainsKey(code))
                    {
                        throw new RiakInvalidDataException((byte)code);
                    }

#if DEBUG
                    // This message code validation is here to make sure that the caller
                    // is getting exactly what they expect. This "could" be removed from
                    // production code, but it's a good thing to have in here for dev.
                    if(MessageCodeToTypeMap[code] != typeof(T))
                    {
                        throw new InvalidOperationException(string.Format("Attempt to decode message to type '{0}' when received type '{1}'.", typeof(T).Name, MessageCodeToTypeMap[code].Name));
                    }
#endif
                    return DeserializeInstance<T>(size).Result;
                });

        }

        private Task<bool> Connect()
        {
            if (_pbcSocket == null)
            {
                var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                    {
                        NoDelay = true
                    };

                return Task.Factory.StartNew(() =>
                    {
                        if (!socket.ConnectAsync(_saea))
                        {
                            throw new RiakException("Unable to connect to remote server: {0}:{1}".Fmt(_server, _port));
                        }

                        _signal.WaitOne();
                        return IsConnected;
                    });
            }

            return true.ToTask();
        }

        private Task<T> DeserializeInstance<T>(int size)
            where T : new()
        {
            if(size <= 1)
            {
                var tcs = new TaskCompletionSource<T>();
                tcs.SetResult(new T());
                return tcs.Task;
            }

            var readSize = size - 1;
            return Read(new byte[readSize], readSize)
                .ContinueWith(t =>
                    {
                        if (t.Result.Item2 != readSize)
                        {
                            throw new RiakException("Unable to read data from the source stream - Timed Out.");
                        }

                        using(var memStream = new MemoryStream(t.Result.Item1))
                        {
                            return Serializer.Deserialize<T>(memStream);
                        }
                    });
        }

        public void Disconnect()
        {
            if(_pbcSocket != null)
            {
                _pbcSocket.Disconnect(false);
                _pbcSocket.Dispose();
                _pbcSocket = null;
            }
        }

        public void Dispose()
        {
            Disconnect();
        }
    }
}
