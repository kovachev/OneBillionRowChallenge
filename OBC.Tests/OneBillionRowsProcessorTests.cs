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
        var measurements = await processor.ProcessFileAsync(inputFilePath);
        //var measurements = await processor.ProcessFileAsync(inputFilePath, processorCount: 1);

        var actualOutput = measurements.ToOutputString();
        
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
    
    [TestMethod]
    [DataRow("C:\\Stefan\\Projects\\OneBillionRowChallenge\\Data\\measurements-100_000.txt")]
    public async Task TestForDebugging(string inputFilePath)
    {
        var processor = new OneBillionRowsProcessor();
        var measurementsSingleThread = await processor.ProcessFileAsync(inputFilePath, processorCount: 1);
        var measurements = await processor.ProcessFileAsync(inputFilePath);

        if (measurements.Count() != measurementsSingleThread.Count())
        {
            Console.WriteLine("The counts are different.");
            return;
        }
        
        const double tolerance = 0.0001;
        
        for (var i = 0; i < measurements.Count(); i++)
        {
            var m1 = measurements.ElementAt(i);
            var m2 = measurementsSingleThread.ElementAt(i);
            
            if (m1.Name != m2.Name)
            {
                Console.WriteLine($"The names are different at index {i}.");
                return;
            }
            
            if (Math.Abs(m1.MinValue - m2.MinValue) > tolerance)
            {
                Console.WriteLine($"The min values are different at index {i}.");
                return;
            }
            
            if (Math.Abs(m1.Average - m2.Average) > tolerance)
            {
                Console.WriteLine($"The average values are different at index {i}.");
                return;
            }
            
            if (Math.Abs(m1.MaxValue - m2.MaxValue) > tolerance)
            {
                Console.WriteLine($"The max values are different at index {i}.");
                return;
            }
            
            if (Math.Abs(m1.Sum - m2.Sum) > tolerance)
            {
                Console.WriteLine($"The sum values are different at index {i}.");
                return;
            }
            
            if (m1.Count != m2.Count)
            {
                Console.WriteLine($"The counts are different at index {i}.");
                return;
            }
        }
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