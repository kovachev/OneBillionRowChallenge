namespace OBC.Core;

internal record Measurement
{
    public string Name { get; init; }
    
    public float MinValue { get; private set; } = float.MaxValue;

    public float MaxValue { get; private set; } = float.MinValue;

    public float Average => Sum / Count;

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
    
    public override string ToString() => $"{Name}={MinValue:#.0}/{Average:#.0}/{MaxValue:#.0}";
}