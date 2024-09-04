using OBC.Core;

namespace OBC.Tests;

[TestClass]
public class OneBillionRowsProcessorTests
{
    [TestMethod]
    [DataRow(
        "C:\\Stefan\\Projects\\OneBillionRowChallenge\\Data\\measurements-1.txt", 
        "C:\\Stefan\\Projects\\OneBillionRowChallenge\\Data\\ExpectedOutput\\measurements-1.out")]
    [DataRow(
        "C:\\Stefan\\Projects\\OneBillionRowChallenge\\Data\\measurements-2.txt", 
        "C:\\Stefan\\Projects\\OneBillionRowChallenge\\Data\\ExpectedOutput\\measurements-2.out")]
    [DataRow(
        "C:\\Stefan\\Projects\\OneBillionRowChallenge\\Data\\measurements-3.txt", 
        "C:\\Stefan\\Projects\\OneBillionRowChallenge\\Data\\ExpectedOutput\\measurements-3.out")]
    [DataRow(
        "C:\\Stefan\\Projects\\OneBillionRowChallenge\\Data\\measurements-10.txt", 
        "C:\\Stefan\\Projects\\OneBillionRowChallenge\\Data\\ExpectedOutput\\measurements-10.out")]
    [DataRow(
        "C:\\Stefan\\Projects\\OneBillionRowChallenge\\Data\\measurements-100_000.txt", 
        "C:\\Stefan\\Projects\\OneBillionRowChallenge\\Data\\ExpectedOutput\\measurements-100_000.out")]
    [DataRow(
        "C:\\Stefan\\Projects\\OneBillionRowChallenge\\Data\\measurements-1_000_000.txt", 
        "C:\\Stefan\\Projects\\OneBillionRowChallenge\\Data\\ExpectedOutput\\measurements-1_000_000.out")]
    public async Task TestWithKnownData(string inputFilePath, string expectedOutputFilePath)
    {
        var processor = new OneBillionRowsProcessor();
        var actualOutput = await processor.ProcessFileAsync(inputFilePath);
        //var actualOutput = await processor.ProcessFileAsync(inputFilePath, processorCount: 1);

        var expectedOutput = await File.ReadAllTextAsync(expectedOutputFilePath);

        var hasFailed = false;
        try
        {
            Assert.AreEqual(expectedOutput.Trim(), actualOutput);
        }
        catch
        {
            hasFailed = true;
            FindDifferences(expectedOutput.Trim(), actualOutput);
        }
        
        Assert.IsFalse(hasFailed);
    }
    
    private static void FindDifferences(string expectedOutput, string actualOutput)
    {
        const int differenceSurroundLength = 25;

        Console.WriteLine("=== Differences ===");
        
        var count = 0;
        for (var i = 0; i < expectedOutput.Length; i++)
        {
            if (i >= actualOutput.Length)
            {
                Console.WriteLine("The expected output is longer.");
                return;
            }
            
            if (expectedOutput[i] != actualOutput[i])
            {
                Console.WriteLine($"Difference {count++} at index {i}.");
                
                var start = i - differenceSurroundLength;
                if (start < 0) start = 0;
                var end = i + differenceSurroundLength;
                
                Console.WriteLine("Expected:");
                if (end >= expectedOutput.Length) end = expectedOutput.Length - 1;
                Console.WriteLine(expectedOutput.Substring(start, end - start));
                
                Console.WriteLine("Actual:");
                end = i + differenceSurroundLength;
                if (end >= actualOutput.Length) end = actualOutput.Length - 1;
                Console.WriteLine(actualOutput.Substring(start, end - start));
                
                Console.WriteLine();
            }
        }
        
        Console.WriteLine("=== End of differences ===");
    }
}