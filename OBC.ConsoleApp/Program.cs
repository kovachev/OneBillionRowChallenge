using System.Diagnostics;
using System.Text;
using OBC.Core;

namespace OBC.ConsoleApp;

internal sealed class Program
{
    internal static async Task Main(string? inputFilePath, string? outputFilePath)
    {
        Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.RealTime;
        Console.OutputEncoding = Encoding.UTF8;

        inputFilePath = inputFilePath?.Trim('\'');
        if (string.IsNullOrWhiteSpace(inputFilePath))
        {
            throw new ArgumentException("Input file path is missing.", nameof(inputFilePath));
        }
        
        var processor = new OneBillionRowsProcessor();
        await processor.ProcessFileAsync(inputFilePath, outputFilePath, processorCount: 1);
    }
}