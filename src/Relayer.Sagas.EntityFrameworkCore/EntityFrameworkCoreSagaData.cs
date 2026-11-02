namespace Relayer.Sagas.EntityFrameworkCore;

/// <summary>Process data using PostgreSQL's database-generated xmin concurrency token.</summary>
public abstract class EntityFrameworkCoreSagaData : SagaData
{
    /// <summary>The native concurrency token. Configure Property(s => s.Version).IsRowVersion().</summary>
    public uint Version { get; set; }
}
