using System;
using System.Collections.Generic;
using System.Text;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using NapCatSharpLib.Event;
using NapCatSharpLib.Event.Manager;

namespace NapCatSharpLib.WebSocket
{
    public interface INapCatWSDebugOutput
    {
        void Log(string message);
    }


    public class NapCatWebSocket
    {
        readonly string ip;
        readonly int port;

        ClientWebSocket client;
        CancellationTokenSource cancellationTokenSource;

        INapCatWSDebugOutput output;

        NapCatEventAnalyzer eventAnalyzer;
        UTF8Encoding utf8 = new UTF8Encoding(false);

        public delegate void EvtDataReceive(string data);
        public EvtDataReceive OnDataReceive;

        public NapCatEventManager EventManager { get; private set; }
        public bool IsConnected { get; private set; } = false;

        public NapCatWebSocket(string _ip, int _port, INapCatWSDebugOutput _debugOutput = null)
        {
            ip = _ip;
            port = _port;
            output = _debugOutput;

            cancellationTokenSource = new CancellationTokenSource();

            EventManager = new NapCatEventManager();

            eventAnalyzer = new NapCatEventAnalyzer(EventManager);
        }

        private async Task MainLoop()
        {
            byte[] buffer;
            while (!cancellationTokenSource.IsCancellationRequested)
            {
                try
                {
                    buffer = new byte[4096 * 16];
                    var result = await client.ReceiveAsync(new ArraySegment<byte>(buffer), cancellationTokenSource.Token);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        output?.Log($"WS Server closed connection: {result.CloseStatus} {result.CloseStatusDescription}");
                        break;
                    }

                    string data = utf8.GetString(buffer, 0, result.Count);
                    output?.Log(data);
                    OnDataReceive?.Invoke(data);

                    try
                    {
                        eventAnalyzer.AnalyzeEvent(data);
                    }
                    catch (Exception e)
                    {
                        output?.Log($"Error Analyzing Event : {e.Message}\n{e.StackTrace}");
                    }
                }
                catch (OperationCanceledException)
                {
                    output?.Log("WS Receive canceled");
                    break;
                }
                catch (WebSocketException ex)
                {
                    output?.Log($"WS Error: {ex.Message}");
                    break;
                }
                catch (ObjectDisposedException)
                {
                    output?.Log("WS Client disposed");
                    break;
                }
                catch (Exception ex)
                {
                    output?.Log($"WS Unexpected error: {ex.Message}");
                    break;
                }
            }

            IsConnected = false;
            output?.Log("WS MainLoop exited");
        }

        public void Start()
        {
            client = new ClientWebSocket();

            var uri = new Uri($"ws://{ip}:{port}");
            output?.Log($"WS Connecting to {uri}...");

            client.ConnectAsync(uri, cancellationTokenSource.Token).ContinueWith(task =>
            {
                if (task.IsFaulted)
                {
                    IsConnected = false;
                    var ex = task.Exception?.InnerException;
                    while (ex != null)
                    {
                        output?.Log($"WS Connect Failed: [{ex.GetType().Name}] {ex.Message}");
                        ex = ex.InnerException;
                    }
                    return;
                }
                if (task.IsCanceled)
                {
                    IsConnected = false;
                    output?.Log("WS Connect Canceled");
                    return;
                }

                IsConnected = true;
                output?.Log("WS Connected, entering main loop");
                _ = MainLoop();
            });

            output?.Log("WS Start");
        }

        public async Task RequestAsync(string data)
        {
            if (!IsConnected || client?.State != WebSocketState.Open)
            {
                output?.Log($"WS RequestAsync skipped: IsConnected={IsConnected}, State={client?.State}");
                return;
            }

            byte[] byteData = utf8.GetBytes(data);
            ArraySegment<byte> reqData = new ArraySegment<byte>(byteData, 0, byteData.Length);
            await client.SendAsync(reqData, WebSocketMessageType.Text, true, cancellationTokenSource.Token);
        }
    }
}
