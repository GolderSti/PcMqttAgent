using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace PcMqttAgent.UI
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
        }

        private async void BtnGetStatus_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // Получаем экземпляр нашего класса App
                var app = (App)Application.Current;
                
                // Вызываем метод и ждем его выполнения
                await app.PipeGetStatus();
            }
            catch (Exception ex)
            {
                // Обработка ошибок отправки (например, обрыв связи)
                MessageBox.Show($"Ошибка запроса статуса: {ex.Message}");
            }
        }
    }
}