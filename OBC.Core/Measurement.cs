using System.Text;

namespace OBC.Core;

public record Measurement
{
    private string Name { get; }

    private float MinValue { get; set; } = float.MaxValue;

    private float MaxValue { get; set; } = float.MinValue;

    private float Average => Sum / Count;

    private float Sum { get; set; }

    private int Count { get; set; }
    
    private object Lock { get; } = new();
    
    public Measurement(string name)
    {
        Name = name;
    }
        
    public void AddValue(float value)
    {
        lock (Lock)
        {
            MinValue = Math.Min(MinValue, value);
            MaxValue = Math.Max(MaxValue, value);
            Sum += value;
            Count++;
        }
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