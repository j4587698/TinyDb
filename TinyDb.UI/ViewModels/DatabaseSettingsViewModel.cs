using System;
using System.ComponentModel;
using System.IO;
using System.Threading.Tasks;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using TinyDb.Core;
using Avalonia.Controls;

namespace TinyDb.UI.ViewModels;

public partial class DatabaseSettingsViewModel : ViewModelBase
{
    private string? _databasePath;
    private bool _enableWal = true;
    private string _walFileNameFormat = "{name}-wal.{ext}";
    private bool _readOnly = false;
    private int _pageSizeKb = 8;
    private string _statusMessage = "配置数据库选项";

    public string? DatabasePath
    {
        get => _databasePath;
        set
        {
            if (SetProperty(ref _databasePath, value))
            {
                UpdateWalFileNamePreview();
            }
        }
    }

    public bool EnableWal
    {
        get => _enableWal;
        set
        {
            if (SetProperty(ref _enableWal, value))
            {
                UpdateWalFileNamePreview();
            }
        }
    }

    public string WalFileNameFormat
    {
        get => _walFileNameFormat;
        set
        {
            if (SetProperty(ref _walFileNameFormat, value))
            {
                UpdateWalFileNamePreview();
            }
        }
    }

    public bool ReadOnly
    {
        get => _readOnly;
        set => SetProperty(ref _readOnly, value);
    }

    public int PageSizeKb
    {
        get => _pageSizeKb;
        set => SetProperty(ref _pageSizeKb, value);
    }

    public string StatusMessage
    {
        get => _statusMessage;
        set => SetProperty(ref _statusMessage, value);
    }

    [ObservableProperty]
    private string _walFileNamePreview = "test-wal.db";

    public DatabaseSettingsViewModel()
    {
        // 设置默认值
        UpdateWalFileNamePreview();
    }

    [RelayCommand]
    private void SetDefaultFormat()
    {
        WalFileNameFormat = "{name}-wal.{ext}";
        StatusMessage = "已设置为默认格式";
    }

    [RelayCommand]
    private void SetLegacyFormat()
    {
        WalFileNameFormat = "{name}-wal.db";
        StatusMessage = "已设置为兼容模式格式";
    }

    [RelayCommand]
    private void Cancel()
    {
        // 关闭窗口，不保存设置
        if (OwnerWindow is Window window)
        {
            window.Close(false);
        }
    }

    [RelayCommand]
    private async Task Save()
    {
        try
        {
            StatusMessage = "正在保存设置...";

            // 这里可以添加保存设置的逻辑
            // 实际应用中，这些设置应该在连接数据库时使用

            await Task.Delay(500); // 模拟保存操作

            StatusMessage = "设置已保存";

            if (OwnerWindow is Window window)
            {
                window.Close(true);
            }
        }
        catch (Exception ex)
        {
            StatusMessage = $"保存失败: {ex.Message}";
        }
    }

    public Window? OwnerWindow { get; set; }

    /// <summary>
    /// 从TinyDbOptions加载设置
    /// </summary>
    public void LoadFromOptions(TinyDbOptions options)
    {
        EnableWal = options.EnableJournaling;
        WalFileNameFormat = options.WalFileNameFormat ?? "{name}-wal.{ext}";
        ReadOnly = options.ReadOnly;
        PageSizeKb = (int)(options.PageSize / 1024);

        UpdateWalFileNamePreview();
    }

    /// <summary>
    /// 保存到TinyDbOptions
    /// </summary>
    public TinyDbOptions SaveToOptions()
    {
        return new TinyDbOptions
        {
            EnableJournaling = EnableWal,
            WalFileNameFormat = string.IsNullOrWhiteSpace(WalFileNameFormat) ? null : WalFileNameFormat,
            ReadOnly = ReadOnly,
            PageSize = (uint)(PageSizeKb * 1024)
        };
    }

    private void UpdateWalFileNamePreview()
    {
        if (string.IsNullOrWhiteSpace(DatabasePath))
        {
            WalFileNamePreview = "test-wal.db";
            return;
        }

        if (!EnableWal)
        {
            WalFileNamePreview = "WAL已禁用";
            return;
        }

        try
        {
            var directory = Path.GetDirectoryName(DatabasePath) ?? string.Empty;
            var fileNameWithoutExt = Path.GetFileNameWithoutExtension(DatabasePath);
            var extension = Path.GetExtension(DatabasePath).TrimStart('.');

            var formattedFileName = WalFileNameFormat
                .Replace("{name}", fileNameWithoutExt)
                .Replace("{ext}", extension);

            if (!Path.HasExtension(formattedFileName) && !string.IsNullOrEmpty(extension))
            {
                formattedFileName += $".{extension}";
            }

            WalFileNamePreview = Path.GetFileName(formattedFileName);
        }
        catch
        {
            WalFileNamePreview = "格式错误";
        }
    }
}