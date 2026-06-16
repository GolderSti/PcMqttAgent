using System;
using System.Text.Json;
using System.Threading.Tasks;
using PcMqttAgent.Models.Pipe;
using PcMqttAgent.Services;

Console.WriteLine("=== Тестовый клиент Named Pipes ===");

using var client = new PipeClient();

// Подписываемся на события
client.ConnectionStatusChanged += payload =>
    Console.WriteLine($"\n[EVENT] Статус MQTT: {(payload.IsConnected ? "ON" : "OFF")} ({payload.Server})");

client.TelemetryReceived += payload =>
    Console.WriteLine($"\n[EVENT] Телеметрия: Uptime={payload.Uptime}, Датчиков={payload.Sensors.Count}");

client.StatusReceived += payload =>
    Console.WriteLine($"\n[RESPONSE] Статус службы: Версия={payload.Version}, Активных датчиков={payload.EnabledSensorsCount}");

try
{
    Console.WriteLine("Подключение...");
    await client.ConnectAsync();
    Console.WriteLine("✓ Подключено! Ожидаем события...\n");

    // Ждем 2 секунды, чтобы получить начальные события (ConnectionStatus, Telemetry)
    await Task.Delay(2000);

    // Отправляем запрос
    Console.WriteLine("Отправка запроса GetStatus...");
    await client.SendAsync(new PipeMessage { Type = PipeMessageType.GetStatus });

    Console.WriteLine("\nНажмите любую клавишу для выхода...");
    Console.ReadKey();
}
catch (Exception ex)
{
    Console.WriteLine($"Ошибка: {ex.Message}");
}
finally
{
    await client.DisconnectAsync();
}