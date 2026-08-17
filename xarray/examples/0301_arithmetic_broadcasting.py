"""Arithmetic and broadcasting: operations align by dimension name, not position.

What: runs arithmetic on a (time, y, x) temperature field — scalar ops, a
field-minus-time-mean anomaly, an outer product of a time series with a spatial
map, and a degC-to-degF conversion — showing that xarray matches dims by NAME.

Why: open-climate-service computes anomalies and derived variables directly on
(time, y, x) stores. Because broadcasting is by dim name, "subtract the time
mean" is one line with no reshape/newaxis bookkeeping, and it cannot silently
pair the wrong axes the way positional numpy broadcasting can.

Run: make run EXAMPLE=0301_arithmetic_broadcasting
"""

from playground_xarray import temperature_dataset


def main() -> None:
    """Demonstrate name-based broadcasting with scalar, anomaly, and unit-conversion ops."""
    t2m = temperature_dataset(days=31)["t2m"]
    print("Daily 2 m temperature field:")
    print(f"  dims={t2m.dims}, shape={t2m.shape}, mean={float(t2m.mean()):.3f} degC")

    # SECTION: scalar arithmetic
    print("\nScalar ops apply elementwise, shape unchanged (e.g. a +0.5 degC bias correction):")
    corrected = t2m + 0.5
    print(f"  (t2m + 0.5): shape={corrected.shape}, mean={float(corrected.mean()):.3f} degC")
    print(f"  attrs ride along unchanged: corrected.attrs['units']={corrected.attrs['units']!r}")

    # SECTION: broadcasting by dim name -- anomaly
    print("\nAnomaly = field minus its time mean. The time mean has dims (y, x);")
    print("xarray lines it up with the (time, y, x) field by NAME and broadcasts over time:")
    time_mean = t2m.mean(dim="time")
    anomaly = t2m - time_mean
    print(f"  time_mean: dims={time_mean.dims}, shape={time_mean.shape}")
    print(f"  anomaly:   dims={anomaly.dims}, shape={anomaly.shape}")
    print(f"  anomaly.mean(dim='time') ~ 0 everywhere: max abs = {float(abs(anomaly.mean(dim='time')).max()):.2e}")
    print("  in numpy this needs values - values.mean(axis=0) -- get the axis wrong and it still 'works'")

    # SECTION: broadcasting builds new dims -- outer product by name
    print("\nOperands with DISJOINT dims broadcast into their union (an outer product by name):")
    series = t2m.mean(dim=["y", "x"])  # dims: (time,)
    spatial = t2m.mean(dim="time")  # dims: (y, x)
    outer = series * spatial
    print(f"  (time,) * (y, x) -> dims={outer.dims}, shape={outer.shape}")
    print("  dim order in the operands is irrelevant; only the names matter")

    # SECTION: unit-style conversion
    print("\nUnit conversions are plain arithmetic (xarray never interprets units attrs):")
    t2m_f = t2m * 9.0 / 5.0 + 32.0
    print(f"  degC: mean={float(t2m.mean()):.3f}, min={float(t2m.min()):.3f}, max={float(t2m.max()):.3f}")
    print(f"  degF: mean={float(t2m_f.mean()):.3f}, min={float(t2m_f.min()):.3f}, max={float(t2m_f.max()):.3f}")
    print(f"  danger: attrs are copied verbatim, so units are now STALE: {t2m_f.attrs['units']!r} on degF values")
    t2m_f.attrs["units"] = "degF"
    print(f"  fix them yourself after converting: t2m_f.attrs['units'] = {t2m_f.attrs['units']!r}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- arithmetic is elementwise; scalars apply everywhere, shape unchanged")
    print("- broadcasting matches dims by NAME: (y, x) lines up under (time, y, x) automatically")
    print("- anomaly = field - field.mean(dim='time') is one line, no reshape/newaxis")
    print("- disjoint dims broadcast into their union: (time,) * (y, x) -> (time, y, x)")
    print("- xarray never interprets attrs: unit conversions copy stale units, so update them yourself")


if __name__ == "__main__":
    main()
