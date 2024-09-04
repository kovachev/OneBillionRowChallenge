namespace OBC.Core;

public record Measurement
{
    public string Name { get; init; }

    public double MinValue { get; set; } = double.MaxValue;

    public double MaxValue { get; set; } = double.MinValue;

    public double Average => Sum / Count;

    public double Sum { get; set; }

    public int Count { get; set; }

    public Measurement(string name)
    {
        Name = name;
    }
        
    public void AddValue(double value)
    {
        MinValue = Math.Min(MinValue, value);
        MaxValue = Math.Max(MaxValue, value);
        Sum += value;
        Count++;
    }
    
    public override string ToString() => $"{Name}={MinValue:F1}/{Average:F1}/{MaxValue:F1}";
}