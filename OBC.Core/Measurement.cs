using System.Text;

namespace OBC.Core;

public record Measurement
{
    public string Name { get; init; }

    public double MinValue { get; set; } = double.MaxValue;

    public double MaxValue { get; set; } = double.MinValue;

    public double Average => Sum / Count;

    public double Sum { get; set; }

    public int Count { get; set; }

    public List<double> Values { get; } = new();
    
    public Measurement(string name)
    {
        Name = name;
    }
        
    public void AddValue(double value)
    {
        MinValue = Math.Min(MinValue, value);
        MaxValue = Math.Max(MaxValue, value);
        Values.Add(value);
        Sum += value;
        Count++;
    }
    
    public override string ToString() => $"{Name}={MinValue:F1}/{AverageToString()}/{MaxValue:F1}";

    public string DebugString()
    {
        var sb = new StringBuilder();
        
        sb.AppendLine($"Name: {Name}");
        sb.AppendLine($"      Min: {MinValue:F10}");
        sb.AppendLine($"      Max: {MaxValue:F10}");
        sb.AppendLine($"      Sum: {Sum:F10}");
        sb.AppendLine($"    Count: {Count:F0}");
        sb.AppendLine($"  Average: {Average:F10}");
        sb.AppendLine($"   Values: {string.Join(", ", Values.Select(x => x.ToString("F1")))}");

        return sb.ToString();
    }

    private string AverageToString()
    {
        var average = $"{Average:0.0}";
        
        if (average == "-0.0")
        {
            average = "0.0";
        }

        return average;
    }
}