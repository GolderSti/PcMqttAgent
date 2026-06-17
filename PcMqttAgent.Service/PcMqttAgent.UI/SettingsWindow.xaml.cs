using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using PcMqttAgent.Models;
using Serilog;

namespace PcMqttAgent.UI;

public partial class SettingsWindow : Window
{
    private readonly ICollectionView _sensorsView;
    private readonly AppSettings _editableSettings;

    public SettingsWindow()
    {
        InitializeComponent();
        
        // Создаем копию настроек для редактирования (чтобы "Отмена" работала корректно)
        _editableSettings = DeepCloneSettings(App.Settings);
        DataContext = _editableSettings;

        // Настраиваем представление для DataGrid с поддержкой фильтрации
        _sensorsView = CollectionViewSource.GetDefaultView(_editableSettings.Sensors);
        _sensorsView.Filter = SensorFilter;
        SensorsGrid.ItemsSource = _sensorsView;

        UpdateCount();
    }

    private AppSettings DeepCloneSettings(AppSettings source)
    {
        // Глубокое копирование через сериализацию
        var json = System.Text.Json.JsonSerializer.Serialize(source);
        return System.Text.Json.JsonSerializer.Deserialize<AppSettings>(json) ?? new AppSettings();
    }

    private void SearchBox_TextChanged(object sender, TextChangedEventArgs e)
    {
        _sensorsView.Refresh();
        UpdateCount();
    }

    private bool SensorFilter(object obj)
    {
        if (obj is not SensorConfig sensor) return false;
        
        string filter = SearchBox.Text.Trim().ToLowerInvariant();
        if (string.IsNullOrEmpty(filter)) return true;

        return sensor.SensorName.ToLowerInvariant().Contains(filter) ||
               sensor.HardwareType.ToLowerInvariant().Contains(filter) ||
               sensor.SensorType.ToLowerInvariant().Contains(filter) ||
               sensor.TopicSuffix.ToLowerInvariant().Contains(filter);
    }

    private void UpdateCount()
    {
        int total = _editableSettings.Sensors.Count;
        int visible = _sensorsView.Cast<object>().Count();
        int enabled = _editableSettings.Sensors.Count(s => s.Enabled);
        
        CountText.Text = $"Показано: {visible} из {total} | Активных: {enabled}";
    }

    private void Save_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            // Валидация базовых полей
            if (string.IsNullOrWhiteSpace(_editableSettings.Mqtt.Server))
            {
                MessageBox.Show("Поле 'Сервер' не может быть пустым.", "Ошибка валидации", 
                    MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            if (_editableSettings.Publisher.IntervalSeconds < 5)
            {
                MessageBox.Show("Интервал отправки должен быть не менее 5 секунд.", "Ошибка валидации", 
                    MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            // Копируем изменения обратно в глобальные настройки
            CopySettings(_editableSettings, App.Settings);
            
            // Сохраняем в файл через статический метод App (если он есть) или напрямую
            SaveToFile(App.Settings);
            
            Log.Information("Настройки сохранены пользователем через UI.");
            
            MessageBox.Show(
                "Настройки сохранены.\n\nДля применения изменений MQTT-подключения требуется перезапуск приложения.", 
                "Успех", MessageBoxButton.OK, MessageBoxImage.Information);
            
            this.Close();
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Ошибка при сохранении настроек из UI.");
            MessageBox.Show($"Ошибка сохранения: {ex.Message}", "Ошибка", 
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void CopySettings(AppSettings source, AppSettings target)
    {
        // MQTT
        target.Mqtt.Server = source.Mqtt.Server;
        target.Mqtt.Port = source.Mqtt.Port;
        target.Mqtt.ClientId = source.Mqtt.ClientId;
        target.Mqtt.User = source.Mqtt.User;
        target.Mqtt.Password = source.Mqtt.Password;
        target.Mqtt.BaseTopic = source.Mqtt.BaseTopic;
        
        // Publisher
        target.Publisher.IntervalSeconds = source.Publisher.IntervalSeconds;
        
        // Logging
        target.Logging.FilePath = source.Logging.FilePath;
        target.Logging.LogLevel = source.Logging.LogLevel;
        
        // ShutdownWarningSeconds
        target.ShutdownWarningSeconds = source.ShutdownWarningSeconds;
        
        // Sensors - заменяем список целиком
        target.Sensors = source.Sensors.Select(s => new SensorConfig
        {
            HardwareType = s.HardwareType,
            SensorType = s.SensorType,
            SensorName = s.SensorName,
            TopicSuffix = s.TopicSuffix,
            Enabled = s.Enabled
        }).ToList();
    }

    private void SaveToFile(AppSettings settings)
    {
        var options = new System.Text.Json.JsonSerializerOptions { WriteIndented = true };
        var json = System.Text.Json.JsonSerializer.Serialize(settings, options);
        string configPath = Path.Combine("C:\\ProgramData\\PcMqttAgent\\", "appsettings.json");
        File.WriteAllText(configPath, json);
    }

    private void Cancel_Click(object sender, RoutedEventArgs e)
    {
        this.Close();
    }
}