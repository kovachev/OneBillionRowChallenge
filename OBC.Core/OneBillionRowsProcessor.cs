using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;

namespace OBC.Core;

public class OneBillionRowsProcessor
{
    private const byte LineEnd = (byte)'\n';
    
    private readonly ConcurrentDictionary<string, Measurement> _measurements = new();

    public async Task ProcessFileAsync(string inputFilePath, string? outputFilePath)
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
            
            AlignFileChunksToNewLine(mappedFile, fileInfo.Length, fileChunks);
            
            Console.WriteLine($"Elapsed after AlignFileChunksToNewLine(): {Stopwatch.GetElapsedTime(timestamp)}");
            
            var tasks = new List<Task>();

            foreach (var fileChunk in fileChunks)
            {
                var sameMappedFile = mappedFile;
                var task = Task.Run(() => ProcessChunk(sameMappedFile, fileChunk));
                
                tasks.Add(task);
            }

            await Task.WhenAll(tasks);
        }
        finally
        {
            mappedFile.Dispose();
        }

        var output = $"{{{string.Join(", ", _measurements.OrderBy(x => x.Key, StringComparer.Ordinal).Select(x => x.Value))}}}";
        
        Console.WriteLine(output);

        if (!string.IsNullOrWhiteSpace(outputFilePath))
        {
            await File.WriteAllTextAsync(outputFilePath, output);
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
    
    private void ProcessChunk(MemoryMappedFile mappedFile, FileChunk fileChunk)
    {
        using var stream = mappedFile.CreateViewStream(fileChunk.Start, fileChunk.Length, MemoryMappedFileAccess.Read);
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

            if (!_measurements.TryGetValue(station, out var measurement))
            {
                measurement = new Measurement(station);
                _measurements.TryAdd(station, measurement);
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