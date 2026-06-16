using System;
using System.Windows;
using System.Windows.Threading;

namespace PcMqttAgent.UI;

public partial class ShutdownDialog : Window
{
    private int _secondsLeft;
    private DispatcherTimer _timer;
    private bool _isConfirmed = false;

    public ShutdownDialog(string action, int timeoutSeconds)
    {
        InitializeComponent();

        string actionText = action.Equals("shutdown", StringComparison.OrdinalIgnoreCase) ? "выключение" : "перезагрузку";
        TxtMessage.Text = $"Получен удаленный запрос на {actionText} компьютера.";

        _secondsLeft = timeoutSeconds;
        UpdateTimerText();

        _timer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(1) };
        _timer.Tick += Timer_Tick;
        _timer.Start();
    }

    private void Timer_Tick(object? sender, EventArgs e)
    {
        _secondsLeft--;
        UpdateTimerText();

        if (_secondsLeft <= 0)
        {
            _timer.Stop();
            _isConfirmed = true;
            DialogResult = true;
            Close();
        }
    }

    private void UpdateTimerText()
    {
        TxtTimer.Text = $"Действие будет выполнено автоматически через: {_secondsLeft} сек.";
    }

    private void BtnConfirm_Click(object sender, RoutedEventArgs e)
    {
        _isConfirmed = true;
        _timer.Stop();
        DialogResult = true;
        Close();
    }

    private void BtnCancel_Click(object sender, RoutedEventArgs e)
    {
        _isConfirmed = false;
        _timer.Stop();
        DialogResult = false;
        Close();
    }

    protected override void OnClosed(EventArgs e)
    {
        _timer?.Stop();
        base.OnClosed(e);
    }
}