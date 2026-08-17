"""Version matching: why client and workers must run the same libraries.

What: compares the package versions on the client, the scheduler, and every
worker, and explains what goes wrong when they drift apart.

Why: this is the most common cause of baffling distributed failures. The
client pickles a function and its arguments; the worker unpickles them with a
different version of numpy or pandas and either crashes, warns, or -- worst --
silently computes something slightly different. This project's Dockerfile pins
the cluster to the client's exact versions, which is why connecting here is
quiet. A deployed open-climate-service instance has the same requirement
between its API image and its worker image.

Run: make run EXAMPLE=0102_versions
"""

from typing import Any

from playground_data_dask_distributed import connect

# The packages whose versions actually matter on the wire: the framework
# itself, the serializers it uses, and the array/dataframe libraries whose
# objects get pickled between processes.
WATCHED = ("python", "dask", "distributed", "numpy", "pandas", "cloudpickle", "msgpack", "toolz")


def _packages(section: Any) -> dict[str, str]:
    """Pull the package/version mapping out of one get_versions() section.

    Args:
        section: The client, scheduler, or a single worker entry from
            ``client.get_versions()``.

    Returns:
        A mapping of package name to version string, empty if the section has
        no packages recorded.
    """
    if not isinstance(section, dict):
        return {}
    packages = section.get("packages", {})
    return {str(k): str(v) for k, v in packages.items()} if isinstance(packages, dict) else {}


def main() -> None:
    """Compare client, scheduler, and worker versions side by side."""
    with connect() as session:
        client = session.client

        # SECTION: gather versions from every participant
        print(session.banner())
        print()
        print("client.get_versions() asks every participant what it is running.")
        # check=False collects the data without raising on a mismatch, which is
        # what we want: we are here to look at the differences, not trip on them.
        versions: dict[str, Any] = client.get_versions(check=False)

        client_pkgs = _packages(versions.get("client"))
        scheduler_pkgs = _packages(versions.get("scheduler"))
        worker_sections = versions.get("workers", {})
        worker_pkgs = [_packages(v) for v in worker_sections.values()] if isinstance(worker_sections, dict) else []

        print(f"  client, scheduler, and {len(worker_pkgs)} worker(s) reported in.\n")

        # SECTION: the comparison table
        print(f"  {'package':<14} {'client':<16} {'scheduler':<16} {'workers':<16} match")
        print(f"  {'-' * 14} {'-' * 16} {'-' * 16} {'-' * 16} -----")
        mismatches: list[str] = []
        for name in WATCHED:
            client_version = client_pkgs.get(name, "-")
            scheduler_version = scheduler_pkgs.get(name, "-")
            worker_versions = {pkgs.get(name, "-") for pkgs in worker_pkgs}
            worker_display = worker_versions.pop() if len(worker_versions) == 1 else "MIXED"
            agree = client_version == scheduler_version == worker_display
            if not agree:
                mismatches.append(name)
            verdict = "yes" if agree else "NO"
            print(f"  {name:<14} {client_version:<16} {scheduler_version:<16} {worker_display:<16} {verdict}")

        # SECTION: what the result means
        if mismatches:
            print(f"\nMismatched: {', '.join(mismatches)}")
            print("distributed raises a VersionMismatchWarning on connect when it sees this.")
            print("Fix it by pinning the image and the client environment to the same versions.")
        else:
            print("\nEverything agrees, so no VersionMismatchWarning on connect.")
            print("That is not luck: the Dockerfile pins numpy, pandas, tornado and friends")
            print("to the versions in uv.lock. The base image alone was a patch behind and")
            print("warned on every single connect.")

        # SECTION: what a mismatch actually costs
        print("\nWhat drift causes, in rough order of how nasty it is:")
        print("  1. a warning on connect -- easy to ignore, and people do")
        print("  2. an unpickling error deep in a task, surfacing as a confusing traceback")
        print("  3. a missing module on the worker: the client imports it fine, the worker cannot")
        print("  4. silently different numerics between library versions -- the one that bites hardest")

        if not session.is_compose:
            print("\nNote: the fallback cluster runs IN this process, so the versions are")
            print("trivially identical. Start the containers (make up) to see a real comparison.")

        # SECTION: summary
        print("\n=== Summary ===")
        print("- client.get_versions(check=False) reports client, scheduler, and workers")
        print("- pin the worker image to the client's locked versions, and bump them together")
        print("- every library used inside a submitted function must exist on the worker too")
        print("- a quiet connect is a feature you have to build, not a default you get")


if __name__ == "__main__":
    main()
