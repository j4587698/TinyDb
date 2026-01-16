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

    [ObservableProperty]
    private DatabaseInfo? _databaseInfo;

    [ObservableProperty]
    private ObservableCollection<CollectionInfo> _collections = new();

    [ObservableProperty]
    private CollectionInfo? _selectedCollection;

    partial void OnSelectedCollectionChanged(CollectionInfo? value)
    {
        UpdateUIState();
        if (value != null && IsConnected)
        {
            _ = LoadDocumentsAsync(value.Name);
        }
        else
        {
            Documents.Clear();
            DocumentContent = string.Empty;
        }
    }

    [ObservableProperty]
    private ObservableCollection<DocumentItem> _documents = new();

    [ObservableProperty]
    private DocumentItem? _selectedDocument;

    [ObservableProperty]
    private ObservableCollection<string> _documentFields = new();

    [ObservableProperty]
    private ObservableCollection<DocumentRow> _documentRows = new();

    partial void OnSelectedDocumentChanged(DocumentItem? value)
    {
        UpdateUIState();
        DocumentContent = value?.Content ?? string.Empty;
    }

    [ObservableProperty]
    private string _documentContent = string.Empty;

    [ObservableProperty]
    private bool _isConnected;

    partial void OnIsConnectedChanged(bool value) => UpdateUIState();

    [ObservableProperty]
    private string _statusMessage = "未连接到数据库";

    [ObservableProperty]
    private string _databasePath = string.Empty;

    partial void OnDatabasePathChanged(string value) => UpdateUIState();

    [ObservableProperty]
    private string _password = string.Empty;

    [ObservableProperty]
    private bool _isLoading;

    [ObservableProperty]
    private bool _canManageTableStructure;

    // UI状态属性
    private bool _canConnect;
    private bool _canDisconnect;
    private bool _canManageCollections;
    private bool _canDeleteCollection;
    private bool _canManageDocuments;
    private bool _canDeleteDocument;
    private bool _canEditDocument;

    public bool CanConnect
    {
        get => _canConnect;
        private set => SetProperty(ref _canConnect, value);
    }

    public bool CanDisconnect
    {
        get => _canDisconnect;
        private set => SetProperty(ref _canDisconnect, value);
    }

    public bool CanManageCollections
    {
        get => _canManageCollections;
        private set => SetProperty(ref _canManageCollections, value);
    }

    public bool CanDeleteCollection
    {
        get => _canDeleteCollection;
        private set => SetProperty(ref _canDeleteCollection, value);
    }

    public bool CanManageDocuments
    {
        get => _canManageDocuments;
        private set => SetProperty(ref _canManageDocuments, value);
    }

    public bool CanDeleteDocument
    {
        get => _canDeleteDocument;
        private set => SetProperty(ref _canDeleteDocument, value);
    }

    public bool CanEditDocument
    {
        get => _canEditDocument;
        private set => SetProperty(ref _canEditDocument, value);
    }

    // 更新UI状态的方法
    private void UpdateUIState()
    {
        CanConnect = !IsConnected && !string.IsNullOrWhiteSpace(DatabasePath);
        CanDisconnect = IsConnected;
        CanManageCollections = IsConnected;
        CanDeleteCollection = IsConnected && SelectedCollection != null;
        CanManageDocuments = IsConnected && SelectedCollection != null;
        CanDeleteDocument = IsConnected && SelectedDocument != null;
        CanEditDocument = IsConnected && SelectedDocument != null;
        CanManageTableStructure = IsConnected;
    }

    public MainWindowViewModel()
    {
        _databaseService = new DatabaseService();
        StatusMessage = "准备就绪，请输入数据库文件路径";
        UpdateUIState();
    }

    [RelayCommand]
    private async Task ConnectAsync()
    {
        if (string.IsNullOrWhiteSpace(DatabasePath))
        {
            StatusMessage = "请输入数据库文件路径";
            return;
        }

        try
        {
            IsLoading = true;
            StatusMessage = "正在连接数据库...";

            var success = await _databaseService.ConnectAsync(DatabasePath,
                string.IsNullOrWhiteSpace(Password) ? null : Password);

            if (success)
            {
                IsConnected = true;
                await LoadDatabaseInfoAsync();
                await LoadCollectionsAsync();

                // 显示详细的连接诊断信息
                var dbInfo = _databaseService.GetDatabaseInfo();
                var diagnosticInfo = $"已连接: {dbInfo.DatabaseName} | " +
                                   $"文件: {dbInfo.FileSize}字节 | " +
                                   $"页使用: {dbInfo.UsedPages}/{dbInfo.TotalPages} | " +
                                   $"表数量: {dbInfo.CollectionCount}";

                if (!string.IsNullOrEmpty(DatabaseService.LastDebugInfo))
                {
                    diagnosticInfo += $" | {DatabaseService.LastDebugInfo}";
                }

                StatusMessage = diagnosticInfo;
            }
            else
            {
                StatusMessage = "连接数据库失败";
            }
        }
        catch (Exception ex)
        {
            StatusMessage = $"连接失败: {ex.Message}";
            IsConnected = false;
        }
        finally
        {
            IsLoading = false;
            UpdateUIState();
        }
    }

    [RelayCommand]
    private void Disconnect()
    {
        try
        {
            _databaseService.Disconnect();
            IsConnected = false;
            DatabaseInfo = null;
            Collections.Clear();
            Documents.Clear();
            SelectedCollection = null;
            SelectedDocument = null;
            DocumentContent = string.Empty;
            StatusMessage = "已断开数据库连接";
            UpdateUIState();
        }
        catch (Exception ex)
        {
            StatusMessage = $"断开连接失败: {ex.Message}";
        }
    }

    [RelayCommand]
    private async Task RecreateDatabase()
    {
        if (string.IsNullOrWhiteSpace(DatabasePath))
        {
            StatusMessage = "请输入数据库文件路径";
            return;
        }

        try
        {
            IsLoading = true;
            StatusMessage = "正在重新创建数据库...";

            var success = await _databaseService.RecreateDatabaseAsync(DatabasePath,
                string.IsNullOrWhiteSpace(Password) ? null : Password);

            if (success)
            {
                IsConnected = true;
                await LoadDatabaseInfoAsync();
                await LoadCollectionsAsync();
                StatusMessage = $"已重新创建数据库: {DatabasePath}";
            }
        }
        catch (Exception ex)
        {
            StatusMessage = $"重新创建数据库失败: {ex.Message}";
        }
        finally
        {
            IsLoading = false;
            UpdateUIState();
        }
    }

    [RelayCommand]
    private async Task SelectDatabaseFile()
    {
        try
        {
            // 暂时使用一个简单的默认路径，用户可以手动编辑
            // 在实际部署中，文件对话框需要更复杂的实现
            var defaultPath = "tinydb_manager.db";
            DatabasePath = defaultPath;
            StatusMessage = $"已设置默认路径: {defaultPath}，您可以手动修改为完整路径";
        }
        catch (Exception ex)
        {
            StatusMessage = $"设置路径失败: {ex.Message}";
        }
    }

    [RelayCommand]
    private async Task RefreshCollections()
    {
        if (!IsConnected) return;

        try
        {
            IsLoading = true;
            await LoadCollectionsAsync();
            StatusMessage = "集合列表已刷新";
        }
        catch (Exception ex)
        {
            StatusMessage = $"刷新集合失败: {ex.Message}";
        }
        finally
        {
            IsLoading = false;
        }
    }

    [RelayCommand]
    private async Task RefreshDocuments()
    {
        if (!IsConnected || SelectedCollection == null) return;

        try
        {
            IsLoading = true;
            await LoadDocumentsAsync(SelectedCollection.Name);
            StatusMessage = $"已刷新 {SelectedCollection.Name} 集合的文档";
        }
        catch (Exception ex)
        {
            StatusMessage = $"刷新文档失败: {ex.Message}";
        }
        finally
        {
            IsLoading = false;
        }
    }

    [RelayCommand]
    private async Task CreateCollection()
    {
        if (!IsConnected) return;

        try
        {
            IsLoading = true;
            var collectionName = $"TestCollection_{DateTime.Now:yyyyMMdd_HHmmss}";

            // 提供更详细的创建过程反馈
            StatusMessage = $"正在创建集合: {collectionName}...";
            Console.WriteLine($"[UI-INFO] 开始创建集合: {collectionName}");

            // 创建集合
            var success = await _databaseService.CreateCollectionAsync(collectionName);

            if (success)
            {
                Console.WriteLine($"[UI-INFO] 集合创建成功: {collectionName}");
                StatusMessage = $"集合创建成功，正在刷新列表...";

                // 短暂延迟确保数据库操作完成
                await Task.Delay(200);

                // 刷新集合列表
                await LoadCollectionsAsync();

                // 自动选择新创建的集合
                var newCollection = Collections.FirstOrDefault(c => c.Name == collectionName);
                if (newCollection != null)
                {
                    SelectedCollection = newCollection;
                    Console.WriteLine($"[UI-INFO] 自动选择新集合: {collectionName}");
                    StatusMessage = $"✅ 已创建并选择集合: {collectionName}";
                }
                else
                {
                    StatusMessage = $"✅ 已创建集合: {collectionName}（请手动选择）";
                }
            }
            else
            {
                StatusMessage = $"❌ 集合创建失败: {collectionName}";
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[UI-ERROR] 创建集合异常: {ex.Message}");
            StatusMessage = $"❌ 创建集合失败: {ex.Message}";

            // 在出错时也尝试刷新集合列表，以防显示过期信息
            try
            {
                await LoadCollectionsAsync();
            }
            catch
            {
                // 忽略刷新错误，主要错误信息更重要
            }
        }
        finally
        {
            IsLoading = false;
            UpdateUIState();
        }
    }

    [RelayCommand]
    private async Task DeleteCollection()
    {
        if (!IsConnected || SelectedCollection == null) return;

        try
        {
            IsLoading = true;
            await _databaseService.DropCollectionAsync(SelectedCollection.Name);
            await LoadCollectionsAsync();
            if (Documents.Any())
            {
                Documents.Clear();
            }
            StatusMessage = $"已删除集合: {SelectedCollection.Name}";
        }
        catch (Exception ex)
        {
            StatusMessage = $"删除集合失败: {ex.Message}";
        }
        finally
        {
            IsLoading = false;
        }
    }

    [RelayCommand]
    private async Task CreateDocument()
    {
        Console.WriteLine($"[DEBUG] CreateDocument called - IsConnected: {IsConnected}, SelectedCollection: {SelectedCollection?.Name}");
        if (!IsConnected || SelectedCollection == null)
        {
            Console.WriteLine($"[DEBUG] CreateDocument early return - IsConnected: {IsConnected}, SelectedCollection: {SelectedCollection?.Name}");
            return;
        }

        try
        {
            IsLoading = true;
            Console.WriteLine($"[DEBUG] Creating document in collection: {SelectedCollection.Name}");
            var jsonContent = @"{
                ""name"": ""New Document"",
                ""description"": ""Created from TinyDb Manager"",
                ""created_at"": """ + DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ") + @""",
                ""tags"": [""test"", ""manager""]
            }";

            var id = await _databaseService.InsertDocumentAsync(SelectedCollection.Name, jsonContent);
            Console.WriteLine($"[DEBUG] Document created with ID: {id}");

            // 重新加载文档列表
            await LoadDocumentsAsync(SelectedCollection.Name);
            StatusMessage = $"已创建文档，ID: {id}";
            Console.WriteLine($"[DEBUG] Document list reloaded, status message set");
        }
        catch (Exception ex)
        {
            StatusMessage = $"创建文档失败: {ex.Message}";
            // 尝试刷新集合列表，可能是集合不存在
            try
            {
                await LoadCollectionsAsync();
            }
            catch
            {
                // 忽略刷新错误
            }
        }
        finally
        {
            IsLoading = false;
        }
    }

    [RelayCommand]
    private async Task UpdateDocument()
    {
        if (!IsConnected || SelectedCollection == null || SelectedDocument == null) return;

        try
        {
            IsLoading = true;
            var success = await _databaseService.UpdateDocumentAsync(
                SelectedCollection.Name,
                SelectedDocument.Id,
                DocumentContent);

            if (success)
            {
                await LoadDocumentsAsync(SelectedCollection.Name);
                StatusMessage = $"已更新文档: {SelectedDocument.Id}";
            }
            else
            {
                StatusMessage = "更新文档失败: 文档可能不存在";
            }
        }
        catch (Exception ex)
        {
            StatusMessage = $"更新文档失败: {ex.Message}";
        }
        finally
        {
            IsLoading = false;
        }
    }

    [RelayCommand]
    private async Task DeleteDocument()
    {
        if (!IsConnected || SelectedCollection == null || SelectedDocument == null) return;

        try
        {
            IsLoading = true;
            var success = await _databaseService.DeleteDocumentAsync(
                SelectedCollection.Name,
                SelectedDocument.Id);

            if (success)
            {
                await LoadDocumentsAsync(SelectedCollection.Name);
                DocumentContent = string.Empty;
                StatusMessage = $"已删除文档: {SelectedDocument.Id}";
            }
            else
            {
                StatusMessage = "删除文档失败: 文档可能不存在";
            }
        }
        catch (Exception ex)
        {
            StatusMessage = $"删除文档失败: {ex.Message}";
        }
        finally
        {
            IsLoading = false;
        }
    }

    [RelayCommand]
    private void OpenTableStructureWindow()
    {
        StatusMessage = "表结构管理按钮被点击..."; // 调试信息
        try
        {
            if (!IsConnected || _databaseService.Engine == null)
            {
                StatusMessage = "请先连接到数据库";
                return;
            }

            // 创建表结构管理窗口
            var tableStructureWindow = new Views.TableStructureWindow(_databaseService, _databaseService.Engine);

            // 设置窗口位置
            tableStructureWindow.WindowStartupLocation = WindowStartupLocation.CenterOwner;

            // 显示窗口
            tableStructureWindow.Show();
            StatusMessage = "表结构管理窗口已打开";
        }
        catch (Exception ex)
        {
            StatusMessage = $"打开表结构窗口失败: {ex.Message}";
        }
    }

    [RelayCommand]
    private async Task OpenDatabaseSettings()
    {
        try
        {
            StatusMessage = "正在打开数据库设置...";

            // 创建数据库设置窗口
            var settingsWindow = new Views.DatabaseSettingsWindow();

            // 创建ViewModel并设置当前数据库路径
            var settingsViewModel = new ViewModels.DatabaseSettingsViewModel();
            if (!string.IsNullOrWhiteSpace(DatabasePath))
            {
                settingsViewModel.DatabasePath = DatabasePath;
            }

            // 设置窗口的DataContext
            settingsWindow.DataContext = settingsViewModel;

            // 设置窗口位置
            settingsWindow.WindowStartupLocation = WindowStartupLocation.CenterOwner;

            // 显示窗口并等待结果
            var result = await settingsWindow.ShowDialog<bool>(Application.Current?.ApplicationLifetime is Avalonia.Controls.ApplicationLifetimes.IClassicDesktopStyleApplicationLifetime desktop ? desktop.MainWindow : null);

            if (result == true)
            {
                StatusMessage = "数据库设置已保存";
                // 在这里可以根据需要处理设置的应用
                // 例如：如果当前已连接数据库，可能需要重新连接以应用新设置
            }
            else
            {
                StatusMessage = "数据库设置已取消";
            }
        }
        catch (Exception ex)
        {
            StatusMessage = $"打开数据库设置失败: {ex.Message}";
        }
    }

    
    private async Task LoadDatabaseInfoAsync()
    {
        try
        {
            DatabaseInfo = _databaseService.GetDatabaseInfo();
        }
        catch (Exception ex)
        {
            StatusMessage = $"加载数据库信息失败: {ex.Message}";
        }
    }

    private async Task LoadCollectionsAsync()
    {
        try
        {
            Console.WriteLine($"[UI-INFO] 开始加载集合列表...");
            StatusMessage = "正在加载表列表...";

            // 添加调试信息
            if (_databaseService.Engine == null)
            {
                Console.WriteLine($"[UI-ERROR] 数据库引擎未初始化");
                StatusMessage = "数据库引擎未初始化";
                return;
            }

            // 清除之前的调试信息
            DatabaseService.LastDebugInfo = null;

            // 在后台线程加载集合数据
            var collections = await Task.Run(() =>
            {
                try
                {
                    Console.WriteLine($"[UI-INFO] 调用GetCollections()...");
                    return _databaseService.GetCollections();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[UI-ERROR] GetCollections()失败: {ex.Message}");
                    throw;
                }
            });

            Console.WriteLine($"[UI-INFO] 获取到 {collections.Count} 个集合");

            // 在UI线程上更新集合数据
            await Avalonia.Threading.Dispatcher.UIThread.InvokeAsync(() =>
            {
                // 保存当前选中的集合名称
                var selectedName = SelectedCollection?.Name;

                Collections.Clear();
                foreach (var collection in collections)
                {
                    Collections.Add(collection);
                    Console.WriteLine($"[UI-INFO] 添加集合到UI: {collection.Name}");
                }

                // 尝试恢复之前选中的集合
                if (!string.IsNullOrEmpty(selectedName))
                {
                    var restoredCollection = Collections.FirstOrDefault(c => c.Name == selectedName);
                    if (restoredCollection != null)
                    {
                        SelectedCollection = restoredCollection;
                        Console.WriteLine($"[UI-INFO] 恢复选中集合: {selectedName}");
                    }
                }

                Console.WriteLine($"[UI-INFO] 集合列表UI更新完成，总数: {Collections.Count}");
            });

            // 显示详细的调试信息
            var statusMsg = $"✅ 已加载 {collections.Count} 个表";

            if (!string.IsNullOrEmpty(DatabaseService.LastDebugInfo))
            {
                statusMsg += $" | {DatabaseService.LastDebugInfo}";
            }

            StatusMessage = statusMsg;
            Console.WriteLine($"[UI-INFO] 集合加载完成: {statusMsg}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[UI-ERROR] 加载集合列表失败: {ex.Message}");
            StatusMessage = $"❌ 加载表列表失败: {ex.Message}";

            // 在UI线程上清空集合
            await Avalonia.Threading.Dispatcher.UIThread.InvokeAsync(() =>
            {
                Collections.Clear();
                SelectedCollection = null;
            });
        }
    }

    private async Task LoadDocumentsAsync(string collectionName)
    {
        try
        {
            Console.WriteLine($"[UI-DEBUG] LoadDocumentsAsync called for collection: {collectionName}");
            var documents = await _databaseService.GetDocumentsAsync(collectionName);
            Console.WriteLine($"[UI-DEBUG] Received {documents.Count} documents from service");

            // 分析文档字段
            var allFields = AnalyzeDocumentFields(documents);
            var orderedFields = OrderDocumentFields(allFields);

            // 创建文档行对象
            var documentRows = documents.Select(doc => new DocumentRow(doc, orderedFields)).ToList();

            // 确保在UI线程上更新集合
            await Avalonia.Threading.Dispatcher.UIThread.InvokeAsync(() =>
            {
                Documents.Clear();
                DocumentFields.Clear();
                DocumentRows.Clear();
                Console.WriteLine($"[UI-DEBUG] Documents collection cleared on UI thread, current count: {Documents.Count}");

                foreach (var document in documents)
                {
                    Documents.Add(document);
                    Console.WriteLine($"[UI-DEBUG] Added document on UI thread: ID={document.Id}, Type={document.Type}, Size={document.Size}");
                }

                foreach (var field in orderedFields)
                {
                    DocumentFields.Add(field);
                }

                foreach (var row in documentRows)
                {
                    DocumentRows.Add(row);
                }

                Console.WriteLine($"[UI-DEBUG] Documents collection final count: {Documents.Count}");
                Console.WriteLine($"[UI-DEBUG] Document fields count: {DocumentFields.Count}, fields: [{string.Join(", ", DocumentFields)}]");
                Console.WriteLine($"[UI-DEBUG] Document rows count: {DocumentRows.Count}");
                StatusMessage = $"已加载 {collectionName} 集合的 {documents.Count} 个文档 | 发现 {DocumentFields.Count} 个字段 | UI集合数量: {Documents.Count}";
                Console.WriteLine($"[UI-DEBUG] Status message set: {StatusMessage}");
            });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[UI-DEBUG] LoadDocumentsAsync failed: {ex.Message}");
            await Avalonia.Threading.Dispatcher.UIThread.InvokeAsync(() =>
            {
                StatusMessage = $"加载文档列表失败: {ex.Message}";
                Documents.Clear();
                DocumentFields.Clear();
                DocumentRows.Clear();
            });
        }
    }

    /// <summary>
    /// 分析文档中的所有字段
    /// </summary>
    private static HashSet<string> AnalyzeDocumentFields(IEnumerable<DocumentItem> documents)
    {
        var allFields = new HashSet<string>();

        foreach (var document in documents)
        {
            // 添加系统字段
            allFields.Add("id");
            allFields.Add("type");
            allFields.Add("size");
            allFields.Add("createdat");
            allFields.Add("modifiedat");

            // 添加JSON解析的字段
            foreach (var field in document.ParsedFields.Keys)
            {
                allFields.Add(field);
            }
        }

        return allFields;
    }

    /// <summary>
    /// 对文档字段进行排序，优先显示重要字段
    /// </summary>
    private static List<string> OrderDocumentFields(HashSet<string> allFields)
    {
        var priorityFields = new[] { "id", "name", "title", "type", "description", "createdat", "modifiedat", "size" };
        var orderedFields = new List<string>();

        // 首先添加优先字段（如果存在）
        foreach (var field in priorityFields)
        {
            if (allFields.Contains(field))
            {
                orderedFields.Add(field);
                allFields.Remove(field);
            }
        }

        // 然后按字母顺序添加其余字段
        orderedFields.AddRange(allFields.OrderBy(f => f));

        return orderedFields;
    }

    protected override void OnPropertyChanged(System.ComponentModel.PropertyChangedEventArgs e)
    {
        base.OnPropertyChanged(e);

        if (e.PropertyName == nameof(IsConnected))
        {
            if (!IsConnected)
            {
                DatabaseInfo = null;
                Collections.Clear();
                Documents.Clear();
                SelectedCollection = null;
                SelectedDocument = null;
                DocumentContent = string.Empty;
            }
        }
    }
}
