using System.Diagnostics;
using System.Text;
using OBC.Core;

namespace OBC.ConsoleApp;

internal sealed class Program
{
    internal static async Task Main(string[] args)
    {
        Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.RealTime;
        Console.OutputEncoding = Encoding.UTF8;

        // Parse input arguments
        // Usage: OBC.ConsoleApp.exe -i <input-file-path> -o <output-file-path> -p <processor-count>
        
        if (args.Length < 1)
        {
            throw new ArgumentException("Input file path is missing.", nameof(args));
        }
        
        var inputFilePath = string.Empty;
        string? outputFilePath = null;
        int? processorCount = null;

        for (var index = 0; index < args.Length; index++)
        {
            var arg = args[index];

            switch (arg)
            {
                case "-i":
                    inputFilePath = args[++index];
                    break;
                
                case "-o":
                    outputFilePath = args[++index];
                    break;
                
                case "-p":
                    processorCount = int.Parse(args[++index]);
                    break;
            }
        }
        
        if (string.IsNullOrWhiteSpace(inputFilePath))
        {
            throw new ArgumentException("Input file path is missing.", nameof(args));
        }
        
        var processor = new OneBillionRowsProcessor();
        await processor.ProcessFileAsync(inputFilePath, outputFilePath, processorCount);
    }
}