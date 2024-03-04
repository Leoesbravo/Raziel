using System;
using System.Collections.Generic;

namespace Data;

public partial class Cargo
{
    public string Id { get; set; } = null!;

    public string? CompanyName { get; set; }

    public string CompanyId { get; set; } = null!;

    public decimal Amount { get; set; }

    public string Satatus { get; set; } = null!;

    public DateTime CreatedAt { get; set; }

    public DateTime? UpdatedAt { get; set; }
}
