namespace OBC.ConsoleApp;

internal record FileChunk
{
    public long Start { get; set; }
    
    public long Length { get; set; }
    
    public long End => Start + Length;
    
    public FileChunk(long start, long length)
    {
        Start = start;
        Length = length;
    }
}