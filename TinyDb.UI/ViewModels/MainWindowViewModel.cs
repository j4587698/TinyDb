using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using TinyDb.UI.Models;
using TinyDb.UI.Services;
using Avalonia.Controls;
using Avalonia;

namespace TinyDb.UI.ViewModels;

public partial class MainWindowViewModel : ViewModelBase
{
    private readonly DatabaseService _databaseService;

    [ObservableProperty] private ObservableCollection<CollectionInfo> _collections = new();
    [ObservableProperty] private CollectionInfo? _selectedCollection;
    
    [ObservableProperty] private ObservableCollection<TableTabViewModel> _openTabs = new();
    [ObservableProperty] private TableTabViewModel? _selectedTab;

    [ObservableProperty] private string _statusMessage = "就绪 - 请连接数据库";
    [ObservableProperty] private string _databasePath = string.Empty;
    [ObservableProperty] private string _password = string.Empty;
    [ObservableProperty] private bool _isLoading;
    [ObservableProperty] private bool _isConnected;

    public bool CanConnect => !IsConnected && !string.IsNullOrWhiteSpace(DatabasePath);
    public bool CanDisconnect => IsConnected;

    public MainWindowViewModel()
    {
        _databaseService = new DatabaseService();
        DatabasePath = "tinydb_test.db";
        UpdateUIState();
    }

    partial void OnIsConnectedChanged(bool value) => UpdateUIState();
    partial void OnDatabasePathChanged(string value) => UpdateUIState();

    partial void OnSelectedCollectionChanged(CollectionInfo? value)
    {
        if (value == null) return;
        OpenTableTab(value.Name);
    }

    private void OpenTableTab(string tableName)
    {
        var existingTab = OpenTabs.FirstOrDefault(t => t.TableName == tableName);
        if (existingTab != null) { SelectedTab = existingTab; return; }
        var newTab = new TableTabViewModel(tableName, _databaseService);
        OpenTabs.Add(newTab);
        SelectedTab = newTab;
    }

    [RelayCommand]
    private void CloseTab(TableTabViewModel tab) => OpenTabs.Remove(tab);

    [RelayCommand]
    private async Task ConnectAsync()
    {
        if (string.IsNullOrWhiteSpace(DatabasePath)) return;
        try {
            IsLoading = true;
            StatusMessage = "正在连接...";
            var success = await _databaseService.ConnectAsync(DatabasePath, Password);
            if (success) {
                IsConnected = true;
                await LoadCollectionsAsync();
                StatusMessage = "数据库已连接";
            }
        } catch (Exception ex) { 
            IsConnected = false;
            StatusMessage = "连接出错: " + ex.Message; 
        } finally { IsLoading = false; UpdateUIState(); }
    }

    [RelayCommand]
    private void Disconnect()
    {
        _databaseService.Disconnect();
        IsConnected = false;
        Collections.Clear();
        OpenTabs.Clear();
        StatusMessage = "已断开连接";
        UpdateUIState();
    }

    [RelayCommand]
    private async Task RecreateDatabase()
    {
        try {
            if (System.IO.File.Exists(DatabasePath)) System.IO.File.Delete(DatabasePath);
            await ConnectAsync();
            StatusMessage = "数据库已重置";
        } catch (Exception ex) { StatusMessage = ex.Message; }
    }

    [RelayCommand]
    private void OpenDesignWindow()
    {
        if (!IsConnected || _databaseService.Engine == null) return;
        var win = new Views.TableStructureWindow(_databaseService, _databaseService.Engine);
        win.Closed += async (s, e) => await LoadCollectionsAsync();
        if (win.DataContext is TableStructureViewModel vm) vm.CreateNewTable();
        win.Show();
    }

    [RelayCommand]
    private async Task SelectDatabaseFile() => DatabasePath = "tinydb_test.db";

    private void UpdateUIState()
    {
        OnPropertyChanged(nameof(CanConnect));
        OnPropertyChanged(nameof(CanDisconnect));
    }

    private async Task LoadCollectionsAsync()
    {
        try {
            var cols = await Task.Run(() => _databaseService.GetCollections());
            await Avalonia.Threading.Dispatcher.UIThread.InvokeAsync(() => {
                Collections.Clear();
                foreach (var c in cols) Collections.Add(c);
            });
        } catch { }
    }
}
