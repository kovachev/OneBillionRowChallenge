namespace OBC.Core;

internal record Measurement
{
    private string Name { get; init; }

    private float MinValue { get; set; } = float.MaxValue;

    private float MaxValue { get; set; } = float.MinValue;

    private float Average => Sum / Count;

    private float Sum { get; set; }

    private int Count { get; set; }

    public Measurement(string name)
    {
        Name = name;
    }
        
    public void AddValue(float value)
    {
        MinValue = Math.Min(MinValue, value);
        MaxValue = Math.Max(MaxValue, value);
        Sum += value;
        Count++;
    }
    
    public override string ToString() => $"{Name}={MinValue:#0.0}/{Average:#0.0}/{MaxValue:#0.0}";
}