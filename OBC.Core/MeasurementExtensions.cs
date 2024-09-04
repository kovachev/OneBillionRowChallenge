namespace OBC.Core;

public static class MeasurementExtensions
{
    public static string ToOutputString(this IEnumerable<Measurement> measurements)
    {
        return $"{{{string.Join(", ", measurements)}}}";
    }
    
    public static double DivTenRound(this double value) =>
        Math.Round(value, MidpointRounding.AwayFromZero);
}