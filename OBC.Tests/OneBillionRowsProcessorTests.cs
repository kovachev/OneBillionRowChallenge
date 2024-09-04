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
    public async Task TestWithKnownData(string inputFilePath, string expectedOutputFilePath)
    {
        var processor = new OneBillionRowsProcessor();
        var actualOutput = await processor.ProcessFileAsync(inputFilePath, null);

        var expectedOutput = await File.ReadAllTextAsync(expectedOutputFilePath);

        Assert.AreEqual(expectedOutput.Trim(), actualOutput);
    }
}