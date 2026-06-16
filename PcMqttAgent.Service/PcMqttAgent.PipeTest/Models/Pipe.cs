using System.Text.Json.Serialization;

namespace PcMqttAgent.Models.Pipe;

public class PipeMessage
{
    [JsonPropertyName("type")]
    public string Type { get; set; } = string.Empty;

    [JsonPropertyName("id")]
    public string Id { get; set; } = Guid.NewGuid().ToString();

    [JsonPropertyName("requestId")]
    public string? RequestId { get; set; } // Заполняется только в ответах на запросы

    [JsonPropertyName("payload")]
    public object? Payload { get; set; }
}

public static class PipeMessageType
{
    // --- Запросы (Request) ---
    public const string GetStatus = "GetStatus";

    // --- Ответы (Response) ---
    public const string StatusResponse = "StatusResponse";
    public const string ShutdownResponse = "ShutdownResponse";

    // --- События (Events) ---
    public const string TelemetryUpdate = "TelemetryUpdate";
    public const string ConnectionStatusChanged = "ConnectionStatusChanged";
    public const string ShutdownRequest = "ShutdownRequest";
}

// === Payload для разных типов сообщений ===

/// <summary>
/// Телеметрия (служба → UI)
/// </summary>
public class TelemetryPayload
{
    [JsonPropertyName("sensors")]
    public Dictionary<string, float> Sensors { get; set; } = new();

    [JsonPropertyName("uptime")]
    public string Uptime { get; set; } = string.Empty;

    [JsonPropertyName("version")]
    public string Version { get; set; } = string.Empty;
}

/// <summary>
/// Статус подключения (служба → UI)
/// </summary>
public class ConnectionStatusPayload
{
    [JsonPropertyName("isConnected")]
    public bool IsConnected { get; set; }

    [JsonPropertyName("server")]
    public string Server { get; set; } = string.Empty;
}

/// <summary>
/// Запрос подтверждения выключения (служба → UI)
/// </summary>
public class ShutdownRequestPayload
{
    [JsonPropertyName("action")]
    public string Action { get; set; } = string.Empty; // "shutdown" или "reboot"

    [JsonPropertyName("timeoutSeconds")]
    public int TimeoutSeconds { get; set; } = 30;
}

/// <summary>
/// Ответ на запрос выключения (UI → Служба)
/// </summary>
public class ShutdownResponsePayload
{
    [JsonPropertyName("confirmed")]
    public bool Confirmed { get; set; }

    [JsonPropertyName("action")]
    public string Action { get; set; } = string.Empty;
}

/// <summary>
/// Полный статус службы (ответ на GetStatus)
/// </summary>
public class StatusPayload
{
    [JsonPropertyName("mqttConnected")]
    public bool MqttConnected { get; set; }

    [JsonPropertyName("mqttServer")]
    public string MqttServer { get; set; } = string.Empty;

    [JsonPropertyName("uptime")]
    public string Uptime { get; set; } = string.Empty;

    [JsonPropertyName("version")]
    public string Version { get; set; } = string.Empty;

    [JsonPropertyName("enabledSensorsCount")]
    public int EnabledSensorsCount { get; set; }
}