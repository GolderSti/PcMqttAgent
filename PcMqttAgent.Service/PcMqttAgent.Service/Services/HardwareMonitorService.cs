using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using LibreHardwareMonitor.Hardware;
using PcMqttAgent.Models;
using Serilog;

namespace PcMqttAgent.Services;

public class HardwareMonitorService : IDisposable
{
    private readonly Computer _computer;
    private readonly UpdateVisitor _updateVisitor;

    public HardwareMonitorService()
    {
        _updateVisitor = new UpdateVisitor();
        _computer = new Computer
        {
            IsCpuEnabled = true,
            IsGpuEnabled = true,
            IsMemoryEnabled = true,
            IsMotherboardEnabled = true,
            IsStorageEnabled = true // Добавил, если вдруг понадобятся диски
        };

        _computer.Open();
        _computer.Accept(_updateVisitor);

        Log.Information("Hardware Monitor инициализирован и просканирован.");
    }

    public void Update() => _computer.Accept(_updateVisitor);

    public float? GetValue(SensorConfig config)
    {
        if (!Enum.TryParse<HardwareType>(config.HardwareType, true, out var hwType)) return null;
        if (!Enum.TryParse<SensorType>(config.SensorType, true, out var sType)) return null;

        foreach (var hardware in _computer.Hardware)
        {
            if (hardware.HardwareType == hwType)
            {
                foreach (var sensor in hardware.Sensors)
                {
                    if (sensor.SensorType == sType &&
                        sensor.Name.Equals(config.SensorName, StringComparison.OrdinalIgnoreCase))
                    {
                        return sensor.Value.HasValue ? (float)Math.Round(sensor.Value.Value, 1) : null;
                    }
                }
            }
        }
        return null;
    }

    public IEnumerable<(string HardwareType, string SensorType, string SensorName)> GetAllAvailableSensors()
    {
        var result = new List<(string, string, string)>();
        foreach (var hardware in _computer.Hardware)
        {
            foreach (var sensor in hardware.Sensors)
            {
                result.Add((hardware.HardwareType.ToString(), sensor.SensorType.ToString(), sensor.Name));
            }
        }
        return result;
    }

    public void Dispose()
    {
        _computer.Close();
    }
}

public class UpdateVisitor : IVisitor
{
    public void VisitComputer(IComputer computer) { computer.Traverse(this); }
    public void VisitHardware(IHardware hardware)
    {
        hardware.Update();
        foreach (IHardware subHardware in hardware.SubHardware) subHardware.Accept(this);
    }
    public void VisitSensor(ISensor sensor) { }
    public void VisitParameter(IParameter parameter) { }
}