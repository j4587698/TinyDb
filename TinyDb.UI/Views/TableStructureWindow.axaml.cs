using System;
using Avalonia;
using Avalonia.Controls;
using SukiUI.Controls;
using TinyDb.UI.ViewModels;
using TinyDb.UI.Services;
using TinyDb.Core;

namespace TinyDb.UI.Views;

public partial class TableStructureWindow : SukiWindow
{
    public TableStructureWindow()
    {
        InitializeComponent();
    }

    public TableStructureWindow(DatabaseService databaseService, TinyDbEngine engine) : this()
    {
        var vm = new TableStructureViewModel(databaseService);
        vm.SetEngine(engine);
        
        // 订阅保存成功事件
        vm.SaveSuccess += () => {
            Avalonia.Threading.Dispatcher.UIThread.Post(this.Close);
        };
        
        DataContext = vm;
    }
}