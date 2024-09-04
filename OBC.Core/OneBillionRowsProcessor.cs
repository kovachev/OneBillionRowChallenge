using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;

namespace OBC.Core;

public class OneBillionRowsProcessor
{
    private const byte LineEnd = (byte)'\n';

    private readonly ConcurrentDictionary<string, Measurement> _measurements = new();

    public static OneBillionRowsProcessor Create() => new();

    public async Task<IEnumerable<Measurement>> ProcessFileAsync(
        string inputFilePath,
        string? outputFilePath = null,
        int? processorCount = null)
    {
        var timestamp = Stopwatch.GetTimestamp();

        var mappedFile = MemoryMappedFile.CreateFromFile(inputFilePath, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);

        try
        {
            var fileInfo = new FileInfo(inputFilePath);

            var tasksCount = processorCount ?? (Environment.ProcessorCount / 2);

            var chunkSize = fileInfo.Length / tasksCount;

            if (chunkSize < 128)
            {
                tasksCount = 1;
                chunkSize = fileInfo.Length;
            }

            Console.WriteLine($"Tasks count: {tasksCount}");
            Console.WriteLine($"Chunk size: {chunkSize}");
            Console.WriteLine($"File size: {fileInfo.Length}");

            var fileChunks = new List<FileChunk>();

            for (var i = 0; i < tasksCount; i++)
            {
                var start = i * chunkSize;
                //if (i > 0) start++;
                var size = i == tasksCount - 1 ? fileInfo.Length - start : chunkSize;
                fileChunks.Add(new FileChunk(start, size));
            }

            AlignFileChunksToNewLine(mappedFile, fileInfo.Length, fileChunks);

            PrintFileChunks(fileChunks);

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

        var result = _measurements.OrderBy(x => x.Key, StringComparer.Ordinal)
                                  .Select(x => x.Value);

        if (!string.IsNullOrWhiteSpace(outputFilePath))
        {
            await File.WriteAllTextAsync(outputFilePath, result.ToOutputString());
        }

        var elapsed = Stopwatch.GetElapsedTime(timestamp);

        Console.WriteLine($"Elapsed: {elapsed}");

        return result;
    }

    private static void PrintFileChunks(List<FileChunk> fileChunks)
    {
        for (var index = 0; index < fileChunks.Count; index++)
        {
            var fileChunk = fileChunks[index];
            Console.WriteLine($"Chunk {index}: {fileChunk.Start} - {fileChunk.End} ({fileChunk.Length})");
        }
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
                offset++;

                if (reader.ReadByte() == LineEnd)
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

        while (reader.ReadLine() is { } line)
        {
            var parts = line.Split(';');
            if (parts.Length != 2)
            {
                continue;
            }

            var station = parts[0];
            var value = float.Parse(parts[1]);

            _measurements.GetOrAdd(station, new Measurement(station))
                         .AddValue(value);
        }
    }
}