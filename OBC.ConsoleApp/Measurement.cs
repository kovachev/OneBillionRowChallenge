namespace OBC.ConsoleApp;

internal record Measurement
{
    public float MinValue { get; private set; } = float.MaxValue;

    public float MaxValue { get; private set; } = float.MinValue;

    public float Average => Sum / Count;

    private float Sum { get; set; }

    private int Count { get; set; }

    public void AddValue(float value)
    {
        MinValue = Math.Min(MinValue, value);
        MaxValue = Math.Max(MaxValue, value);
        Sum += value;
        Count++;
    }
}