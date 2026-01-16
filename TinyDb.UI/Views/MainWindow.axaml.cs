using System;
using Avalonia.Controls;
using TinyDb.UI.ViewModels;
using TinyDb.UI.Models;

namespace TinyDb.UI.Views;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
    }

    private void TestBinding_Click(object? sender, Avalonia.Interactivity.RoutedEventArgs e)
    {
        if (DataContext is MainWindowViewModel viewModel)
        {
            // 添加一个测试文档到Documents集合
            var testDoc = new DocumentItem
            {
                Id = $"test_{DateTime.Now:yyyyMMdd_HHmmss}",
                Type = "TestDocument",
                Content = "{\"name\":\"Test Document\",\"description\":\"This is a test document for binding validation\",\"created\":\"" + DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ") + "\"}",
                CollectionName = "TestCollection",
                CreatedAt = DateTime.UtcNow,
                ModifiedAt = DateTime.UtcNow,
                Size = 150
            };

            viewModel.Documents.Add(testDoc);
            viewModel.StatusMessage = $"已添加测试文档: {testDoc.Id} | 集合总数: {viewModel.Documents.Count} | 类型: {testDoc.Type} | 大小: {testDoc.Size}字节";
        }
    }
}