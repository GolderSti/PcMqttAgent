using System.Collections.Generic;

namespace PcMqttAgent.Models;

public class AppSettings
{
    public MqttSettings Mqtt { get; set; } = new();
    public PublisherSettings Publisher { get; set; } = new();
    public LoggingSettings Logging { get; set; } = new();
    public int ShutdownWarningSeconds { get; set; } = 15; // Время на раздумья перед выключением
    
    // Новый список конфигурации датчиков
    public List<SensorConfig> Sensors { get; set; } = new()
    {
    };
}

public class SensorConfig
{
    public string HardwareType { get; set; } = ""; // Например: Cpu, GpuAmd, Memory
    public string SensorType { get; set; } = "";   // Например: Load, Temperature, Data
    public string SensorName { get; set; } = "";   // Точное имя из LibreHardwareMonitor
    public string TopicSuffix { get; set; } = "";  // Хвост топика, например: "temp/cpu"
    public bool Enabled { get; set; } = false;     // Включена ли отправка этого датчика
}

public class MqttSettings
{
    public string Server { get; set; } = "mqtt-server.lan";
    public int Port { get; set; } = 1883;
    public string ClientId { get; set; } = "LAPTOP";
    public string User { get; set; } = "";
    public string Password { get; set; } = "";
    public string BaseTopic { get; set; } = "/LAPTOP";
}

public class PublisherSettings
{
    public int IntervalSeconds { get; set; } = 120;
}

public class LoggingSettings
{
    public string FilePath { get; set; } = "logs/agent-.log";
    public string LogLevel { get; set; } = "Information";
}