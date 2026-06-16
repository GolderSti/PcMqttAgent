using PcMqttAgent.Models;
using PcMqttAgent.Services;
using Serilog;
using Serilog.Events;
using System.Threading.Channels;

var baseDir = AppContext.BaseDirectory;

// 1. Загрузка конфигурации
var config = new ConfigurationBuilder()
    .SetBasePath("C:\\ProgramData\\PcMqttAgent")
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .Build();

var settings = config.Get<AppSettings>() ?? new AppSettings();

// 2. Настройка Serilog
var logPath = Path.Combine("C:\\ProgramData\\PcMqttAgent", settings.Logging.FilePath.Replace("agent-.log", $"agent-{DateTime.Now:yyyyMMdd}.log"));
Directory.CreateDirectory(Path.GetDirectoryName(logPath)!);

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Is(Enum.Parse<LogEventLevel>(settings.Logging.LogLevel))
    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
    .MinimumLevel.Override("System", LogEventLevel.Warning)
    .WriteTo.File(logPath, rollingInterval: RollingInterval.Day, retainedFileCountLimit: 7)
    .CreateLogger();

try
{
    Log.Information("=== Запуск хоста MyApp Agent Service ===");

    var builder = Host.CreateApplicationBuilder(args);

    // Включаем поддержку Windows Service
    builder.Services.AddWindowsService(options =>
    {
        options.ServiceName = "MyAppAgentService";
    });

    // 3. Регистрируем зависимости (DI)

    // Настройки как Singleton (один экземпляр на всё время работы)
    builder.Services.AddSingleton(settings);

    // Монитор железа как Singleton (он держит открытым Computer, это дорого)
    builder.Services.AddSingleton<HardwareMonitorService>();

    // Регистрируем PipeServer как Singleton, чтобы MqttService мог его использовать
    builder.Services.AddSingleton<PipeServerService>();
    builder.Services.AddHostedService(sp => sp.GetRequiredService<PipeServerService>());

    // MqttService регистрируем как HostedService (BackgroundService)
    builder.Services.AddHostedService<MqttService>();

    // Добавляем Serilog в стандартный ILogger (на случай, если понадобится)
    builder.Services.AddSerilog();

    var host = builder.Build();

    // Запуск хоста. Этот метод блокирует выполнение, пока служба не будет остановлена.
    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Хост завершился аварийно");
}
finally
{
    Log.CloseAndFlush();
}