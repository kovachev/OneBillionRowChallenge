using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Text;

namespace OBC.ConsoleApp;

internal sealed class Program
{
    private static readonly ConcurrentDictionary<string, Measurement> Measurements = new();
   
    private const byte LineEnd = (byte)'\n';
    
    internal static async Task Main(string? inputFilePath)
    {
        Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.RealTime;
        Console.OutputEncoding = Encoding.UTF8;

        inputFilePath = inputFilePath?.Trim('\'');
        if (string.IsNullOrWhiteSpace(inputFilePath))
        {
            throw new ArgumentException("Input file path is missing.", nameof(inputFilePath));
        }

        await ProcessFileAsync(inputFilePath);
    }
    
    private static async Task ProcessFileAsync(string inputFilePath)
    {
        var timestamp = Stopwatch.GetTimestamp();
        
        var mappedFile = MemoryMappedFile.CreateFromFile(inputFilePath, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);

        try
        {
            var fileInfo = new FileInfo(inputFilePath);

            var processCount = Environment.ProcessorCount / 2;

            var chunkSize = fileInfo.Length / processCount;

            var fileChunks = new List<FileChunk>();

            for (var i = 0; i < processCount; i++)
            {
                var size = i == processCount - 1 ? fileInfo.Length - i * chunkSize : chunkSize;
                fileChunks.Add(new FileChunk(i * chunkSize, size));
            }
            
            var timestamp2 = Stopwatch.GetTimestamp();
            
            AlignFileChunksToNewLine(mappedFile, fileInfo.Length, fileChunks);
            
            var elapsed2 = Stopwatch.GetElapsedTime(timestamp2);
            
            Console.WriteLine($"AlignFileChunksToNewLine: {elapsed2}");
            
            var tasks = new List<Task>();

            foreach (var chunk in fileChunks)
            {
                var sameMappedFile = mappedFile;
                var task = Task.Run(() => ProcessChunk(sameMappedFile, chunk.Start, chunk.End));
                
                tasks.Add(task);
            }

            await Task.WhenAll(tasks);
        }
        finally
        {
            mappedFile.Dispose();
        }

        foreach (var measurement in Measurements.OrderBy(x => x.Key, StringComparer.Ordinal))
        {
            Console.WriteLine($"{measurement.Key}={measurement.Value.MinValue}/{measurement.Value.Average:#.0}/{measurement.Value.MaxValue}");
        }

        var elapsed = Stopwatch.GetElapsedTime(timestamp);

        Console.WriteLine($"Elapsed: {elapsed}");
    }

    private static void AlignFileChunksToNewLine(MemoryMappedFile mappedFile, long fileSize, List<FileChunk> fileChunks)
    {
        const int searchLength = 128;
        
        if (fileChunks.Count <= 1)
        {
            return;
        }
        
        for (var i = 1; i < fileChunks.Count; i++)
        {
            var fileChunk = fileChunks[i];
            
            var start = fileChunk.Start;
            var end = fileChunk.Start + searchLength;
            
            var size = end > fileSize ? fileSize - start : searchLength;

            using var stream = mappedFile.CreateViewStream(start, size, MemoryMappedFileAccess.Read);
            using var reader = new BinaryReader(stream);

            var offset = 0L;
            while (reader.BaseStream.Position < reader.BaseStream.Length)
            {
                var b = reader.ReadByte();
                offset++;

                if (b == LineEnd)
                {
                    break;
                }
            }

            if (offset != 0)
            {
                fileChunk.Start += offset;
                fileChunk.Length -= offset;
                
                // Adjust previous chunk
                fileChunks[i - 1].Length += offset;
            }
        }
    }
    
    private static void ProcessChunk(MemoryMappedFile mappedFile, long start, long end)
    {
        using var stream = mappedFile.CreateViewStream(start, end - start, MemoryMappedFileAccess.Read);
        using var reader = new StreamReader(stream);

        var counter = 0;
        while (reader.ReadLine() is { } line)
        {
            var parts = line.Split(';');
            if (parts.Length != 2)
            {
                continue;
            }

            var station = parts[0];
            var value = float.Parse(parts[1]);

            if (!Measurements.TryGetValue(station, out var measurement))
            {
                measurement = new Measurement();
                Measurements.TryAdd(station, measurement);
            }

            measurement.AddValue(value);

            counter++;
            if (counter % 100_000_000 == 0)
            {
                Console.WriteLine($"[{Thread.CurrentThread.ManagedThreadId,2}: {counter:##,###}]");
            }
        }
    }
}