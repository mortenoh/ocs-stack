"""CF attribute conventions: units and names as inert metadata.

What: attaches CF-style attrs (units, long_name, standard_name) to data
variables and coordinates by hand, proves xarray never interprets them (a
degC array and a Kelvin array add without complaint), and shows how the
keep_attrs option controls whether operations propagate attrs.

Why: attrs are the contract between producers and consumers of climate data,
but to xarray they are inert strings. That is exactly why open-climate-service
standardizes units once at ingest -- Kelvin sources become Celsius and the
units attr is rewritten to match -- because nothing downstream will ever read
the attr and convert for you.

Run: make run EXAMPLE=0701_cf_attrs_units
"""

import xarray as xr

from playground_xarray import temperature_dataset


def main() -> None:
    """Attach CF attrs by hand and demonstrate that xarray treats them as inert metadata."""
    # SECTION: CF attrs on data variables
    print("A CF-described variable carries units, long_name, and standard_name:")
    ds = temperature_dataset(days=10, ny=3, nx=4)
    ds["t2m"].attrs["standard_name"] = "air_temperature"  # from the CF standard name table
    for key, value in ds["t2m"].attrs.items():
        print(f"  t2m.attrs[{key!r}] = {value!r}")
    print("  units/long_name are free text; standard_name must come from the CF table")

    # SECTION: CF attrs on coordinates
    print("\nCoordinates get the same treatment (units, standard_name, axis):")
    ds["y"].attrs.update({"units": "degrees_north", "standard_name": "latitude", "axis": "Y"})
    ds["x"].attrs.update({"units": "degrees_east", "standard_name": "longitude", "axis": "X"})
    for name in ("y", "x"):
        print(f"  {name}.attrs = {dict(ds[name].attrs)}")
    print("  time gets no units attr: CF time units ('days since ...') live in encoding, applied at write time")

    # SECTION: attrs are inert
    print("\nxarray never reads attrs -- units are decoration, not behavior:")
    celsius = ds["t2m"]
    kelvin = (celsius + 273.15).assign_attrs(units="K", long_name="2 metre temperature")
    mixed = celsius + kelvin  # degC + K: physically meaningless, and no warning is raised
    print(f"  celsius mean          = {float(celsius.mean()):7.2f} [{celsius.attrs['units']}]")
    print(f"  kelvin mean           = {float(kelvin.mean()):7.2f} [{kelvin.attrs['units']}]")
    print(f"  celsius + kelvin mean = {float(mixed.mean()):7.2f} -- nonsense, computed without complaint")
    print("  (a units-aware layer like pint-xarray can check this; plain xarray does not)")

    # SECTION: what operations do to attrs -- the keep_attrs option
    print("\nWhether operations propagate attrs is governed by the keep_attrs option:")
    print(f"  celsius.mean().attrs = {celsius.mean().attrs}")
    print("    (this xarray keeps attrs by default; older releases dropped them on reductions)")
    print(f"  (celsius + kelvin).attrs = {mixed.attrs}")
    print("    (attrs the operands agree on survive; the conflicting 'units' was quietly dropped)")
    with xr.set_options(keep_attrs=False):
        stripped = celsius.mean()
    print(f"  with xr.set_options(keep_attrs=False): celsius.mean().attrs = {stripped.attrs}")
    print("  the default has flipped across versions -- set the option explicitly, never assume")

    # SECTION: kept attrs are copied verbatim, right or wrong
    print("\nPropagation is a blind copy; it cannot know which operations invalidate metadata:")
    with xr.set_options(keep_attrs=True):
        mean = celsius.mean()
        var = celsius.var()
    print(f"  celsius.mean().attrs['units'] = {mean.attrs['units']!r}  (correct: a mean keeps its units)")
    print(f"  celsius.var().attrs['units']  = {var.attrs['units']!r}  (now wrong: variance is degC squared)")

    # SECTION: the ingest-time standardization pattern
    print("\nBecause attrs are inert, OCS converts units once at ingest and rewrites attrs to match:")
    era5_kelvin = kelvin  # pretend this arrived from ERA5, where 2 m temperature is in Kelvin
    ingested = (era5_kelvin - 273.15).assign_attrs(era5_kelvin.attrs, units="degC")
    print(f"  source:   mean {float(era5_kelvin.mean()):7.2f} [{era5_kelvin.attrs['units']}]")
    print(f"  ingested: mean {float(ingested.mean()):7.2f} [{ingested.attrs['units']}]")
    print("  every store then speaks degC; downstream code trusts the convention, not the metadata")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- CF attrs (units, long_name, standard_name, axis) describe variables and coords")
    print("- xarray never interprets attrs: degC + K adds silently -- units are your job")
    print("- xr.set_options(keep_attrs=...) controls propagation; the default flipped across versions")
    print("- propagation is a verbatim copy and can go wrong (var() keeping units='degC')")
    print("- OCS normalizes units at ingest (K -> degC) precisely because attrs are inert")


if __name__ == "__main__":
    main()
