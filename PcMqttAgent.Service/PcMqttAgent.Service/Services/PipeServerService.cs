using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Text;
using System.Text.Json;
using PcMqttAgent.Models.Pipe;
using Serilog;

namespace PcMqttAgent.Services;

public sealed class PipeClientConnection
{
    public string Id { get; init; } = string.Empty;
    public NamedPipeServerStream Pipe { get; init; } = null!;
    public SemaphoreSlim WriteLock { get; } = new(1, 1);
}

public class PipeServerService : BackgroundService
{
    private const string PipeName = "PcMqttAgentPipe";
    private readonly ConcurrentDictionary<string, PipeClientConnection> _clients = new();

    // События для бизнес-логики (MqttService)
    public event Func<string, PipeMessage, Task>? MessageReceived;
    public event Action<string>? ClientConnected;
    public event Action<string>? ClientDisconnected;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Log.Information("PipeServer запущен. Ожидание подключений...");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var server = new NamedPipeServerStream(
                    PipeName,
                    PipeDirection.InOut,
                    NamedPipeServerStream.MaxAllowedServerInstances,
                    PipeTransmissionMode.Byte,
                    PipeOptions.Asynchronous);

                await server.WaitForConnectionAsync(stoppingToken);

                var connection = new PipeClientConnection
                {
                    Id = Guid.NewGuid().ToString(),
                    Pipe = server
                };

                _clients.TryAdd(connection.Id, connection);
                Log.Information("Клиент подключен (ID: {Id})", connection.Id);
                ClientConnected?.Invoke(connection.Id);

                // Запускаем чтение для этого клиента в отдельной задаче
                _ = Task.Run(() => ReadLoopAsync(connection, stoppingToken), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Ошибка в PipeServer. Перезапуск через 2 сек...");
                await Task.Delay(2000, stoppingToken);
            }
        }

        Log.Information("PipeServer остановлен.");
    }

    private async Task ReadLoopAsync(PipeClientConnection connection, CancellationToken stoppingToken)
    {
        try
        {
            using var reader = new StreamReader(connection.Pipe, Encoding.UTF8);

            while (connection.Pipe.IsConnected && !stoppingToken.IsCancellationRequested)
            {
                var line = await reader.ReadLineAsync();
                if (line == null) break; // Клиент закрыл соединение

                try
                {
                    var message = JsonSerializer.Deserialize<PipeMessage>(line);
                    if (message != null)
                    {
                        Log.Debug("Получено сообщение от {Id}: {Type}", connection.Id, message.Type);
                        if (MessageReceived != null)
                        {
                            await MessageReceived.Invoke(connection.Id, message);
                        }
                    }
                }
                catch (JsonException ex)
                {
                    Log.Warning(ex, "Некорректный JSON от клиента {Id}", connection.Id);
                }
            }
        }
        catch (IOException)
        {
            Log.Information("Клиент {Id} отключился (IOException)", connection.Id);
        }
        finally
        {
            RemoveClient(connection.Id);
        }
    }

    private void RemoveClient(string clientId)
    {
        if (_clients.TryRemove(clientId, out var connection))
        {
            connection.Pipe.Dispose();
            connection.WriteLock.Dispose();
            Log.Information("Клиент {Id} удален", clientId);
            ClientDisconnected?.Invoke(clientId);
        }
    }

    public async Task SendToClientAsync(string clientId, PipeMessage message)
    {
        if (!_clients.TryGetValue(clientId, out var connection))
        {
            Log.Warning("Попытка отправки несуществующему клиенту {Id}", clientId);
            return;
        }

        await connection.WriteLock.WaitAsync();
        try
        {
            if (!connection.Pipe.IsConnected) return;

            var json = JsonSerializer.Serialize(message) + "\n";
            var bytes = Encoding.UTF8.GetBytes(json);

            await connection.Pipe.WriteAsync(bytes);
            await connection.Pipe.FlushAsync();
            Log.Information($"Sended {bytes} to {clientId}");
        }
        catch (IOException ex)
        {
            Log.Warning("Ошибка записи клиенту {Id}: {Message}", clientId, ex.Message);
            RemoveClient(clientId);
        }
        finally
        {
            connection.WriteLock.Release();
        }
    }

    public async Task BroadcastAsync(PipeMessage message)
    {
        var tasks = _clients.Keys.Select(id => SendToClientAsync(id, message));
        await Task.WhenAll(tasks);
    }

    public override void Dispose()
    {
        foreach (var conn in _clients.Values)
        {
            conn.Pipe.Dispose();
            conn.WriteLock.Dispose();
        }
        _clients.Clear();
        base.Dispose();
    }
}