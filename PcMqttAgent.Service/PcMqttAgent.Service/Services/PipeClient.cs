using System;
using System.Diagnostics;
using System.IO.Pipes;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PcMqttAgent.Models.Pipe;

namespace PcMqttAgent.Services;

public class PipeClient : IDisposable
{
    private NamedPipeClientStream? _pipe;
    private CancellationTokenSource? _cts;
    private Task? _readerTask;

    // События для ViewModel
    public event Action<TelemetryPayload>? TelemetryReceived;
    public event Action<ConnectionStatusPayload>? ConnectionStatusChanged;
    public event Action<StatusPayload>? StatusReceived;
    public event Action<ShutdownRequestPayload>? ShutdownRequested;

    public bool IsConnected => _pipe?.IsConnected ?? false;

    public async Task ConnectAsync(string serverName = ".", int timeoutMs = 5000)
    {
        _pipe = new NamedPipeClientStream(serverName, "PcMqttAgentPipe", PipeDirection.InOut, PipeOptions.Asynchronous);
        await _pipe.ConnectAsync(timeoutMs);

        _cts = new CancellationTokenSource();
        _readerTask = Task.Run(() => ReaderLoopAsync(_cts.Token));
    }

    public async Task DisconnectAsync()
    {
        _cts?.Cancel();
        if (_readerTask != null)
        {
            try { await _readerTask; } catch { }
        }
        _pipe?.Dispose();
        _pipe = null;
    }

    private async Task ReaderLoopAsync(CancellationToken token)
    {
        using var reader = new StreamReader(_pipe!, Encoding.UTF8);
        while (!token.IsCancellationRequested && _pipe!.IsConnected)
        {
            var line = await reader.ReadLineAsync();
            if (line == null) break;

            try
            {
                Debug.WriteLine($"RAW: {line}");

                var msg = JsonSerializer.Deserialize<PipeMessage>(line);

                if (msg == null)
                {
                    Debug.WriteLine("Message == null");
                    continue;
                }

                Debug.WriteLine($"Type = {msg.Type}");
                Debug.WriteLine($"Payload = {msg.Payload}"); if (msg == null) continue;

                // Маршрутизация событий
                switch (msg.Type)
                {
                    case PipeMessageType.TelemetryUpdate:
                        TelemetryReceived?.Invoke(JsonSerializer.Deserialize<TelemetryPayload>(msg.Payload!.ToString()!)!);
                        break;
                    case PipeMessageType.ConnectionStatusChanged:
                        ConnectionStatusChanged?.Invoke(JsonSerializer.Deserialize<ConnectionStatusPayload>(msg.Payload!.ToString()!)!);
                        break;
                    case PipeMessageType.StatusResponse:
                        StatusReceived?.Invoke(JsonSerializer.Deserialize<StatusPayload>(msg.Payload!.ToString()!)!);
                        break;
                    case PipeMessageType.ShutdownRequest:
                        ShutdownRequested?.Invoke(JsonSerializer.Deserialize<ShutdownRequestPayload>(msg.Payload!.ToString()!)!);
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка парсинга сообщения: {ex.Message}");
            }
        }
    }

    public async Task SendAsync(PipeMessage message)
    {
        if (_pipe == null || !_pipe.IsConnected) throw new InvalidOperationException("Not connected");

        var json = JsonSerializer.Serialize(message) + "\n";
        var bytes = Encoding.UTF8.GetBytes(json);
        await _pipe.WriteAsync(bytes);
        await _pipe.FlushAsync();
    }

    public void Dispose()
    {
        DisconnectAsync().GetAwaiter().GetResult();
        _cts?.Dispose();
    }
}