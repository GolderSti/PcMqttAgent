using Microsoft.Extensions.Hosting; // Для BackgroundService
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using PcMqttAgent.Models;
using PcMqttAgent.Models.Pipe;
using Serilog;
using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;

namespace PcMqttAgent.Services;

// 1. Наследуемся от BackgroundService
public class MqttService : BackgroundService
{
    private readonly AppSettings _settings;
    private readonly HardwareMonitorService _hardwareMonitor;
    private readonly PipeServerService _pipeServer;
    private readonly IMqttClient _mqttClient;

    private static readonly TimeSpan MqttPowerChangeTimeout = TimeSpan.FromSeconds(2);

    // Настройки экспоненциальной задержки
    private const int BaseDelayMs = 2000;
    private const int MaxDelayMs = 60000;
    private const int JitterMs = 2000;
    private int _reconnectAttempts = 0;
    private readonly Random _random = new();

    private Timer? _publishTimer;
    private bool _isReConnecting;
    private bool _isConnected;
    private bool _isStopping;

    private string PowerTopic => $"/{_settings.Mqtt.BaseTopic.Trim('/')}/POWER";
    private string PowerSetTopic => $"/{_settings.Mqtt.BaseTopic.Trim('/')}/POWER/SET";
    private string CommandTopic => $"{_settings.Mqtt.BaseTopic}/command";
    private string AckTopic => $"{_settings.Mqtt.BaseTopic}/command/ack";

    // 2. Внедрение зависимостей через конструктор
    public MqttService(
        AppSettings settings,
        HardwareMonitorService hardwareMonitor,
        PipeServerService pipeServer) // ДОБАВЛЕНО
    {
        _settings = settings;
        _hardwareMonitor = hardwareMonitor;
        _pipeServer = pipeServer; // ДОБАВЛЕНО

        var factory = new MqttFactory();
        _mqttClient = factory.CreateMqttClient();

        _mqttClient.ConnectedAsync += OnConnectedAsync;
        _mqttClient.DisconnectedAsync += OnDisconnectedAsync;
        _mqttClient.ApplicationMessageReceivedAsync += OnMessageReceivedAsync;

        _pipeServer.MessageReceived += HandlePipeMessageAsync;

        //_pipeServer.OnClientConnected += OnPipeClientConnectedAsync;

        //// Подписываемся на сообщения от UI
        //_pipeServer.OnMessageReceived += HandlePipeMessageAsync;
    }

    // 3. Главный метод жизненного цикла службы
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Log.Information("Запуск MQTT сервиса (ExecuteAsync)...");

        // СИНХРОНИЗАЦИЯ ДАТЧИКОВ ПЕРЕД ЗАПУСКОМ
        SyncSensorConfig();

        var options = new MqttClientOptionsBuilder()
            .WithTcpServer(_settings.Mqtt.Server, _settings.Mqtt.Port)
            .WithClientId(_settings.Mqtt.ClientId)
            .WithCredentials(_settings.Mqtt.User, _settings.Mqtt.Password)
            .WithCleanSession()
            .WithWillTopic(PowerTopic)
            .WithWillPayload("OFF")
            .WithWillQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .WithWillRetain(true)
            .Build();

        _isReConnecting = false;
        try
        {
            await _mqttClient.ConnectAsync(options, stoppingToken);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Первичное подключение к MQTT не удалось. Запуск цикла переподключения.");
            _ = Task.Run(() => ReconnectLoopAsync(stoppingToken), stoppingToken);
        }

        // Ждем сигнала остановки службы (например, через sc stop или выключение ПК)
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Это нормальное поведение при остановке службы
        }
        finally
        {
            // Штатное завершение: публикуем OFF и отключаемся
            _isStopping = true;
            await PublishOffAndDisconnectAsync();
        }
    }

    private async Task ReconnectLoopAsync(CancellationToken stoppingToken)
    {
        if (_isReConnecting) return;
        _isReConnecting = true;

        while (!stoppingToken.IsCancellationRequested && !_isStopping && !_mqttClient.IsConnected)
        {
            _reconnectAttempts++;
            try
            {
                await _mqttClient.ConnectAsync(_mqttClient.Options, stoppingToken).ConfigureAwait(false);
                _isReConnecting = false;
                break;
            }
            catch (OperationCanceledException)
            {
                _isReConnecting = false;
                break;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Ошибка переподключения. Будет новая попытка.");
            }

            int delaycnt = Math.Min(_reconnectAttempts, 32);
            int delayMs = (int)Math.Min(MaxDelayMs, BaseDelayMs * Math.Pow(2, delaycnt - 1));
            int jitter = _random.Next(0, JitterMs);
            int totalDelayMs = delayMs + jitter;

            Log.Warning($"Переподключение через {totalDelayMs / 1000.0:F1} сек. (Попытка #{_reconnectAttempts})");
            await Task.Delay(totalDelayMs, stoppingToken);
        }
        _isReConnecting = false;
    }

    private async Task OnConnectedAsync(MqttClientConnectedEventArgs e)
    {
        try
        {
            _isConnected = true;
            _reconnectAttempts = 0;
            Log.Information("Успешно подключено к MQTT брокеру.");

            // 1. Уведомляем UI
            if (_pipeServer != null)
            {
                await _pipeServer.BroadcastAsync(new PipeMessage
                {
                    Type = PipeMessageType.ConnectionStatusChanged,
                    Payload = new ConnectionStatusPayload
                    {
                        IsConnected = true,
                        Server = _settings.Mqtt.Server
                    }
                });
            }

            // 2. Публикуем статус и чистим retain
            await PublishPowerStateAsync("ON").ConfigureAwait(false);
            await ClearRetainedCommandsAsync().ConfigureAwait(false);

            // 3. Подписываемся на топики
            await _mqttClient.SubscribeAsync(new MqttTopicFilterBuilder().WithTopic(CommandTopic).Build()).ConfigureAwait(false);
            await _mqttClient.SubscribeAsync(new MqttTopicFilterBuilder().WithTopic(PowerSetTopic).Build()).ConfigureAwait(false);

            // 4. Запускаем таймер телеметрии
            if (_publishTimer == null)
            {
                _publishTimer = new Timer(
                    async _ => await PublishPeriodicDataAsync().ConfigureAwait(false),
                    null,
                    TimeSpan.FromSeconds(5), // <-- БЫЛО: TimeSpan.Zero
                    TimeSpan.FromSeconds(_settings.Publisher.IntervalSeconds)
                );
            }
            else
            {
                _publishTimer.Change(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(_settings.Publisher.IntervalSeconds));
            }

            Log.Information("Инициализация после подключения к MQTT завершена успешно.");
        }
        catch (Exception ex)
        {
            // Если здесь произойдет ошибка, мы ее поймаем, запишем в лог и принудительно разорвем соединение,
            // чтобы сработал наш надежный ReconnectLoop.
            Log.Error(ex, "КРИТИЧЕСКАЯ ОШИБКА в OnConnectedAsync. Вызываю переподключение.");
            try
            {
                await _mqttClient.DisconnectAsync();
            }
            catch { }
        }
    }
    private async Task OnDisconnectedAsync(MqttClientDisconnectedEventArgs e)
    {
        if (_isConnected)
        {
            Log.Information("Отключено от MQTT брокера.");
        }

        // === ДОБАВЛЕНО: Детальная диагностика причины отключения ===
        Log.Warning("Причина отключения MQTT: {Reason}. Исключение: {Exception}",
            e.Reason,
            e.Exception?.Message ?? "Нет");

        _isConnected = false;
        _publishTimer?.Change(Timeout.Infinite, Timeout.Infinite);

        // === Уведомляем UI ===
        if (_pipeServer != null)
        {
            await _pipeServer.BroadcastAsync(new PipeMessage
            {
                Type = PipeMessageType.ConnectionStatusChanged,
                Payload = new ConnectionStatusPayload
                {
                    IsConnected = false,
                    Server = _settings.Mqtt.Server
                }
            });
        }

        if (_isStopping) return;

        _ = Task.Run(() => ReconnectLoopAsync(CancellationToken.None));
    }
    private async Task ClearRetainedCommandsAsync()
    {
        await PublishSingleAsync(CommandTopic, "", true).ConfigureAwait(false);
        await PublishSingleAsync(PowerSetTopic, "", true).ConfigureAwait(false);
        Log.Information("Retain-команды на брокере очищены.");
    }

    private async Task OnMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs e)
    {
        var payload = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment)
                                    .Replace("\n", "").Replace("\r", "").Trim();
        var topic = e.ApplicationMessage.Topic;

        if (string.IsNullOrWhiteSpace(payload)) return;

        Log.Information($"Получена команда по топику {topic}: '{payload}'");
        await PublishAckAsync(payload).ConfigureAwait(false);

        if (topic.Equals(PowerSetTopic, StringComparison.OrdinalIgnoreCase))
        {
            if (payload.Equals("OFF", StringComparison.OrdinalIgnoreCase))
            {
                await ProcessPowerCommandAsync("shutdown", "/s /f /t 0").ConfigureAwait(false);
            }
            return;
        }

        if (topic.Equals(CommandTopic, StringComparison.OrdinalIgnoreCase))
        {
            switch (payload.ToLowerInvariant())
            {
                case "shutdown":
                    await ProcessPowerCommandAsync("shutdown", "/s /f /t 0").ConfigureAwait(false);
                    break;
                case "reboot":
                case "restart":
                    await ProcessPowerCommandAsync("reboot", "/r /f /t 0").ConfigureAwait(false);
                    break;
                default:
                    Log.Warning($"Неизвестная команда: '{payload}'");
                    break;
            }
        }
    }

    private async Task ProcessPowerCommandAsync(string actionName, string shutdownArgs)
    {
        Log.Warning($"Запрос на {actionName.ToUpper()}. Ожидаем подтверждения от UI...");

        var requestId = Guid.NewGuid().ToString();
        var tcs = new TaskCompletionSource<bool>();

        // Временный обработчик, который ждет именно наш ответ
        async Task ResponseHandler(string clientId, PipeMessage msg)
        {
            if (msg.Type == PipeMessageType.ShutdownResponse && msg.RequestId == requestId)
            {
                try
                {
                    var payload = JsonSerializer.Deserialize<ShutdownResponsePayload>(msg.Payload.ToString()!);
                    tcs.TrySetResult(payload.Confirmed);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Ошибка парсинга ShutdownResponse");
                    tcs.TrySetResult(false);
                }
            }
        }

        _pipeServer.MessageReceived += ResponseHandler;

        try
        {
            // Отправляем запрос всем подключенным UI (вдруг их несколько)
            await _pipeServer.BroadcastAsync(new PipeMessage
            {
                Type = PipeMessageType.ShutdownRequest,
                Id = requestId,
                Payload = new ShutdownRequestPayload
                {
                    Action = actionName,
                    RequestId = requestId,
                    TimeoutSeconds = _settings.ShutdownWarningSeconds
                }
            });

            // Ждем ответа или таймаута (время таймаута + 5 секунд на обработку)
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(_settings.ShutdownWarningSeconds + 5));
            bool confirmed = false;

            try
            {
                confirmed = await tcs.Task.WaitAsync(cts.Token);
            }
            catch (TaskCanceledException)
            {
                Log.Warning("Таймаут ожидания подтверждения от UI. Действие отменено.");
            }
            finally
            {
                // Обязательно отписываемся, чтобы не было утечки памяти
                _pipeServer.MessageReceived -= ResponseHandler;
            }

            if (confirmed)
            {
                Log.Information("Пользователь подтвердил действие. Выполнение...");
                ExecuteSystemCommand(shutdownArgs);
            }
            else
            {
                Log.Information("Пользователь отменил действие или таймаут.");
            }
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Критическая ошибка при обработке запроса выключения");
            _pipeServer.MessageReceived -= ResponseHandler;
        }
    }
    private void ExecuteSystemCommand(string arguments)
    {
        try
        {
            Log.Information($"Выполнение: C:\\Windows\\System32\\shutdown.exe {arguments}");
            var startInfo = new ProcessStartInfo
            {
                FileName = "C:\\Windows\\System32\\shutdown.exe",
                Arguments = arguments,
                UseShellExecute = false,
                CreateNoWindow = true,
                WindowStyle = ProcessWindowStyle.Hidden
            };
            using var process = Process.Start(startInfo);
            if (process == null)
            {
                Log.Error("Не удалось запустить shutdown.exe: Process.Start вернул null.");
            }
            else
            {
                Log.Information("Команда shutdown.exe успешно передана ОС.");
            }
        }
        catch (Exception ex)
        {
            Log.Error(ex, "КРИТИЧЕСКАЯ ОШИБКА: Не удалось запустить shutdown.exe.");
        }
    }

    private async Task PublishPowerStateAsync(string state)
    {
        if (!_isConnected) return;
        await PublishSingleAsync(PowerTopic, state, true).ConfigureAwait(false);
    }

    private async Task PublishAckAsync(string command)
    {
        if (!_isConnected) return;
        await PublishSingleAsync(AckTopic, $"received: {command}", false).ConfigureAwait(false);
    }

    private async Task PublishPeriodicDataAsync()
    {
        if (!_isConnected || _hardwareMonitor == null) return;

        try
        {
            _hardwareMonitor.Update();
            var version = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "1.0.0";
            var uptime = TimeSpan.FromMilliseconds(Environment.TickCount64).ToString(@"dd\.hh\:mm\:ss");

            // Отправка в MQTT
            await PublishSingleAsync($"{_settings.Mqtt.BaseTopic}/info/version", version, true).ConfigureAwait(false);
            await PublishSingleAsync($"{_settings.Mqtt.BaseTopic}/info/uptime", uptime, true).ConfigureAwait(false);

            var telemetryPayload = new TelemetryPayload
            {
                Uptime = uptime,
                Version = version
            };

            foreach (var sensorConfig in _settings.Sensors.Where(s => s.Enabled))
            {
                var value = _hardwareMonitor.GetValue(sensorConfig);
                if (value.HasValue)
                {
                    string formattedValue = FormatValue(value.Value, sensorConfig.SensorType);
                    string topic = $"{_settings.Mqtt.BaseTopic}/info/{sensorConfig.TopicSuffix}";
                    await PublishSingleAsync(topic, formattedValue, true).ConfigureAwait(false);

                    telemetryPayload.Sensors[sensorConfig.TopicSuffix] = value.Value;
                }
            }

            // Отправляем телеметрию в UI
            if (_pipeServer != null)
            {
                var telemetryMessage = new PipeMessage
                {
                    Type = PipeMessageType.TelemetryUpdate,
                    Payload = telemetryPayload
                };
                await _pipeServer.BroadcastAsync(telemetryMessage);
            }
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Ошибка при публикации периодических данных.");
        }
    }
    private string FormatValue(float value, string sensorType)
    {
        if (sensorType.Equals("Temperature", StringComparison.OrdinalIgnoreCase)) return $"{value}";
        if (sensorType.Equals("Load", StringComparison.OrdinalIgnoreCase)) return $"{value}";
        if (sensorType.Equals("Data", StringComparison.OrdinalIgnoreCase)) return $"{value}";
        return value.ToString();
    }

    private async Task PublishSingleAsync(string topic, string payload, bool retain = true)
    {
        var message = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(payload)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .WithRetainFlag(retain)
            .Build();

        await _mqttClient.PublishAsync(message).ConfigureAwait(false);
    }

    private async Task PublishOffAndDisconnectAsync()
    {
        _publishTimer?.Change(Timeout.Infinite, Timeout.Infinite);

        if (!_isConnected) return;

        try
        {
            await PublishPowerStateAsync("OFF").WaitAsync(MqttPowerChangeTimeout).ConfigureAwait(false);
            await _mqttClient.DisconnectAsync().WaitAsync(MqttPowerChangeTimeout).ConfigureAwait(false);
            Log.Information("Служба штатно отключилась от MQTT (OFF published).");
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "Не удалось штатно отключиться от MQTT (возможно, таймаут).");
        }
    }
//=========  Обновляем датчики
    private void SyncSensorConfig()
    {
        Log.Information("Синхронизация конфигурации датчиков...");
        bool configChanged = false;
        var availableSensors = _hardwareMonitor.GetAllAvailableSensors();

        foreach (var avail in availableSensors)
        {
            // Проверяем, есть ли уже такой датчик в конфиге
            bool exists = _settings.Sensors.Any(s =>
                s.HardwareType.Equals(avail.HardwareType, StringComparison.OrdinalIgnoreCase) &&
                s.SensorType.Equals(avail.SensorType, StringComparison.OrdinalIgnoreCase) &&
                s.SensorName.Equals(avail.SensorName, StringComparison.OrdinalIgnoreCase));

            if (!exists)
            {
                // Добавляем новый датчик с Enabled = false
                _settings.Sensors.Add(new SensorConfig
                {
                    HardwareType = avail.HardwareType,
                    SensorType = avail.SensorType,
                    SensorName = avail.SensorName,
                    TopicSuffix = $"auto/{avail.HardwareType.ToLower()}_{avail.SensorType.ToLower()}",
                    Enabled = false // По умолчанию выключен
                });
                configChanged = true;
                Log.Information($"Обнаружен новый датчик: [{avail.HardwareType}] {avail.SensorName} (добавлен в конфиг как Disabled)");
            }
        }

        if (configChanged)
        {
            SaveConfig();
            Log.Information("Файл appsettings.json обновлен новыми датчиками.");
        }
        else
        {
            Log.Information("Конфигурация датчиков актуальна.");
        }
    }

    private void SaveConfig()
    {
        try
        {
            var options = new JsonSerializerOptions { WriteIndented = true };
            var json = JsonSerializer.Serialize(_settings, options);

            // ВАЖНО: Используем AppContext.BaseDirectory, а не AppDomain.CurrentDomain.BaseDirectory
            // Для Windows Service это гарантирует правильный путь к папке с exe-файлом службы
            string configPath = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
            File.WriteAllText(configPath, json);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Не удалось сохранить appsettings.json");
        }
    }
    // Код Pipeline Server============================
    // Обработчик запросов от UI
    private async Task HandlePipeMessageAsync(string clientId, PipeMessage message)
    {
        Log.Information("Обработка сообщения {Type} от клиента {ClientId}", message.Type, clientId);

        if (message.Type == PipeMessageType.GetStatus)
        {
            var payload = new StatusPayload
            {
                MqttConnected = _isConnected,
                MqttServer = _settings.Mqtt.Server,
                Uptime = TimeSpan.FromMilliseconds(Environment.TickCount64).ToString(@"dd\.hh\:mm\:ss"),
                Version = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "1.0.0",
                EnabledSensorsCount = _settings.Sensors.Count(s => s.Enabled)
            };

            var response = new PipeMessage
            {
                Type = PipeMessageType.StatusResponse,
                RequestId = message.Id, // Важно: привязываем ответ к ID запроса
                Payload = payload
            };

            // Отправляем ответ ТОЛЬКО инициатору
            await _pipeServer.SendToClientAsync(clientId, response);
        }
    }

    private async Task HandlePipeMessageAsync(PipeMessage message)
    {
        Log.Debug("Обработка сообщения от UI: {Type}", message.Type);

        switch (message.Type)
        {
            case PipeMessageType.GetStatus:
                await SendStatusResponseAsync(message.Id);
                break;

            case PipeMessageType.ShutdownResponse:
                // Ответ от UI на наш запрос подтверждения
                // Обработаем в следующем шаге, когда вернем диалог
                Log.Information("Получен ответ от UI на shutdown request");
                break;
        }
    }

    private async Task SendStatusResponseAsync(string requestId)
    {
        Log.Information("Формирую и отправляю StatusResponse для requestId: {RequestId}", requestId);
        var payload = new StatusPayload
        {
            MqttConnected = _isConnected,
            MqttServer = _settings.Mqtt.Server,
            Uptime = TimeSpan.FromMilliseconds(Environment.TickCount64).ToString(@"dd\.hh\:mm\:ss"),
            Version = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "1.0.0",
            EnabledSensorsCount = _settings.Sensors.Count(s => s.Enabled)
        };

        var response = new PipeMessage
        {
            Type = PipeMessageType.StatusResponse,
            RequestId = requestId,
            Payload = payload
        };

        await _pipeServer.BroadcastAsync(response);
        Log.Information("StatusResponse успешно помещен в очередь отправки");
    }
    //private async Task OnPipeClientConnectedAsync(string clientId)
    //{
    //    Log.Information("Отправка начального состояния клиенту {ClientId}", clientId);

    //    // Отправляем статус подключения к MQTT
    //    await _pipeServer.BroadcastAsync(new PipeMessage
    //    {
    //        Type = PipeMessageType.ConnectionStatusChanged,
    //        Payload = new ConnectionStatusPayload
    //        {
    //            IsConnected = _isConnected,
    //            Server = _settings.Mqtt.Server
    //        }
    //    });

    //    // Отправляем текущую телеметрию
    //    await SendTelemetryToUiAsync();
    //}
    private async Task SendTelemetryToUiAsync()
    {
        Log.Information("SendTelemetryToUiAsync");
        if (_hardwareMonitor == null) return;

        try
        {
            _hardwareMonitor.Update();
            var version = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "1.0.0";
            var uptime = TimeSpan.FromMilliseconds(Environment.TickCount64).ToString(@"dd\.hh\:mm\:ss");

            var telemetryPayload = new TelemetryPayload
            {
                Uptime = uptime,
                Version = version
            };

            foreach (var sensorConfig in _settings.Sensors.Where(s => s.Enabled))
            {
                var value = _hardwareMonitor.GetValue(sensorConfig);
                if (value.HasValue)
                {
                    telemetryPayload.Sensors[sensorConfig.TopicSuffix] = value.Value;
                }
            }

            await _pipeServer.BroadcastAsync(new PipeMessage
            {
                Type = PipeMessageType.TelemetryUpdate,
                Payload = telemetryPayload
            });

            Log.Information("Телеметрия отправлена в UI: {Count} датчиков", telemetryPayload.Sensors.Count);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Ошибка при отправке телеметрии в UI");
        }
    }
    public override void Dispose()
    {
        _mqttClient?.Dispose();
        _publishTimer?.Dispose();
        base.Dispose();
    }
}