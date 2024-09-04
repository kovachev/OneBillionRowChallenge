using OBC.Core;

namespace OBC.Tests;

[TestClass]
public class OneBillionRowsProcessorTests
{
    private const string BasePath = @"C:\Stefan\Projects\OneBillionRowChallenge\Data\";
    private const string BasePathExpectedOutput = BasePath + @"ExpectedOutput\";
    
    [TestMethod]
    public async Task TestWithKnownData_1()
    {
        await TestWithKnownData("measurements-1");
    }

    [TestMethod]
    public async Task TestWithKnownData_2()
    {
        await TestWithKnownData("measurements-2");
    }

    [TestMethod]
    public async Task TestWithKnownData_3()
    {
        await TestWithKnownData("measurements-3");
    }

    [TestMethod]
    public async Task TestWithKnownData_10()
    {
        await TestWithKnownData("measurements-10");
    }

    [TestMethod]
    public async Task TestWithKnownData_100K()
    {
        await TestWithKnownData("measurements-100_000");
    }

    [TestMethod]
    public async Task TestWithKnownData_1M()
    {
        await TestWithKnownData("measurements-1_000_000");
    }

    [TestMethod]
    public async Task TestWithKnownData_10M()
    {
        await TestWithKnownData("measurements-10_000_000");
    }
    
    [TestMethod]
    public async Task TestWithKnownData_1BRC()
    {
        await TestWithKnownData("measurements-1_000_000_000");
    }
    
    private static async Task TestWithKnownData(
        string inputFileName,
        int? processorCount = null)
    {
        var inputFilePath = BasePath + inputFileName + ".txt";
        var expectedOutputFilePath = BasePathExpectedOutput + inputFileName + ".out";

        var measurements = await OneBillionRowsProcessor.Create()
                                                        .ProcessFileAsync(
                                                            inputFilePath,
                                                            processorCount: processorCount
                                                        );

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

            foreach (var measurement in measurements)
            {
                Console.WriteLine(measurement.DebugString());
            }
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