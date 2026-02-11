using Avalonia.Controls;
using SukiUI.Controls;

namespace TinyDb.UI.Views;

public partial class DatabaseSettingsWindow : SukiWindow
{
    public DatabaseSettingsWindow()
    {
        InitializeComponent();

        // 设置ViewModel的OwnerWindow引用
        if (DataContext is ViewModels.DatabaseSettingsViewModel viewModel)
        {
            viewModel.OwnerWindow = this;
        }
    }
}