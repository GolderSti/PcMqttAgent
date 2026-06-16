using Hardcodet.Wpf.TaskbarNotification;
using PcMqttAgent.Models.Pipe;
using PcMqttAgent.Services;
using System;
using System.Drawing; 
using System.Windows;
using System.Windows.Resources; 

namespace PcMqttAgent.UI;

public partial class App : Application
{
    private PipeClient _pipeClient = null!;
    private TaskbarIcon _trayIcon = null!;
    private MainWindow? _mainWindow;

    protected override async void OnStartup(StartupEventArgs e)
    {
        base.OnStartup(e);

        // 1. Инициализация Tray Icon
        _trayIcon = (TaskbarIcon)FindResource("TrayIcon");
        _trayIcon.ToolTipText = "PcMqttAgent: Подключение к службе...";

        // 2. Инициализация Pipe Client
        _pipeClient = new PipeClient();

        // Подписка на события от службы
        _pipeClient.ConnectionStatusChanged += OnConnectionStatusChanged;
        _pipeClient.TelemetryReceived += OnTelemetryReceived;
        _pipeClient.ShutdownRequested += OnShutdownRequested;
        _pipeClient.StatusReceived += OnStatusReceived;

        try
        {
            await _pipeClient.ConnectAsync();
            _trayIcon.ToolTipText = "PcMqttAgent: Подключено к службе";
            await _pipeClient.SendAsync(new PipeMessage { Type = PipeMessageType.GetStatus });
        }
        catch (Exception ex)
        {
            _trayIcon.ToolTipText = $"PcMqttAgent: Ошибка ({ex.Message})";
            MessageBox.Show($"Не удалось подключиться к службе.\nУбедитесь, что служба PcMqttAgent запущена.\n\n{ex.Message}",
                "Ошибка", MessageBoxButton.OK, MessageBoxImage.Error);
            Current.Shutdown();
            return;
        }

        // 3. Создаем главное окно, но НЕ показываем его (Show() вызовется только по запросу)
        _mainWindow = new MainWindow();
        _mainWindow?.Show();
    }

    private void OnStatusReceived(StatusPayload obj)
    {
        Dispatcher.Invoke(() =>
        {
            _trayIcon.ToolTipText = obj.MqttConnected
                ? $"PcMqttAgent: Online ({obj.MqttServer})"
                : "PcMqttAgent: Offline (Нет связи с брокером)";

            var iconName = obj.MqttConnected
                ? "Resources/icon_connected.ico"
                : "Resources/icon_disconnected.ico";

            var streamInfo = Application.GetResourceStream(
                new Uri(iconName, UriKind.Relative));

            if (streamInfo != null)
            {
                _trayIcon.Icon = new Icon(streamInfo.Stream);
            }
        });
    }
    private void OnConnectionStatusChanged(ConnectionStatusPayload payload)
    {
        Dispatcher.Invoke(() =>
        {
            _trayIcon.ToolTipText = payload.IsConnected
                ? $"PcMqttAgent: Online ({payload.Server})"
                : "PcMqttAgent: Offline (Нет связи с брокером)";

            // 1. Выбираем нужный ресурс
            var iconName = payload.IsConnected ? "Resources/icon_connected.ico" : "Resources/icon_disconnected.ico";
            Uri uri = new Uri(iconName, UriKind.Relative);

            // 2. Получаем поток из WPF-ресурсов
            StreamResourceInfo streamInfo = Application.GetResourceStream(uri);

            // 3. Загружаем как System.Drawing.Icon (сохраняет ВСЕ слои!)
            if (streamInfo != null)
            {
                _trayIcon.Icon = new Icon(streamInfo.Stream);
            }
        });
    }
    private void OnTelemetryReceived(TelemetryPayload payload)
    {
        Dispatcher.Invoke(() =>
        {
            // Обновляем tooltip телеметрией (опционально, чтобы не было слишком длинным)
            // _trayIcon.ToolTipText = $"CPU: {payload.Sensors["..."]} | Uptime: {payload.Uptime}";
        });
    }

    private async void OnShutdownRequested(ShutdownRequestPayload payload)
    {
        // ВАЖНО: Вызываем диалог в UI потоке
        await Dispatcher.InvokeAsync(async () =>
        {
            var dialog = new ShutdownDialog(payload.Action, payload.TimeoutSeconds);
            var result = dialog.ShowDialog();

            // Отправляем ответ обратно в службу
            await _pipeClient.SendAsync(new PipeMessage
            {
                Type = PipeMessageType.ShutdownResponse,
                RequestId = payload.RequestId, // Теперь используем правильный ID
                Payload = new ShutdownResponsePayload { Confirmed = result == true, Action = payload.Action }
            });
        });
    }

    private void ShowStatus_Click(object sender, RoutedEventArgs e)
    {
        //
    }

    private void Exit_Click(object sender, RoutedEventArgs e)
    {
        Current.Shutdown();
    }

    protected override async void OnExit(ExitEventArgs e)
    {
        _trayIcon.Dispose();
        await _pipeClient.DisconnectAsync();
        _pipeClient.Dispose();
        base.OnExit(e);
    }
}