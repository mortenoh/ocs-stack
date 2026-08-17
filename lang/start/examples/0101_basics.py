"""Basics: using the playground_lang_start library.

What: demonstrates the two functions exported by playground_lang_start —
fizzbuzz and slugify — including their error handling.

Why: this example exists to verify the template end to end: the package
imports from an installed src layout, the example runs under `uv run`,
and lint/type checks cover the examples directory.

Run: make run EXAMPLE=0101_basics   (or: uv run python examples/0101_basics.py)
"""

from playground_lang_start import fizzbuzz, slugify


def main() -> None:
    """Run the demos and print a summary."""
    # SECTION: fizzbuzz
    print("fizzbuzz(15) walks 1..15, replacing multiples of 3 and 5:")
    print("  " + " ".join(fizzbuzz(15)))

    print("\nInvalid input raises a clear error:")
    try:
        fizzbuzz(0)
    except ValueError as e:
        print(f"  fizzbuzz(0) -> ValueError: {e}")

    # SECTION: slugify
    print("\nslugify collapses punctuation and whitespace into hyphens:")
    for text in ["Hello, World!", "  uv + ruff + mypy  ", "CHAPKIT-style template"]:
        print(f"  {text!r} -> {slugify(text)!r}")

    print("\nText without alphanumerics is rejected:")
    try:
        slugify("---")
    except ValueError as e:
        print(f"  slugify('---') -> ValueError: {e}")

    # SECTION: summary
    print("\n=== Summary ===")
    print("- fizzbuzz(n) returns a list of strings and rejects n < 1")
    print("- slugify(text) normalizes to a lowercase hyphenated slug")
    print("- both raise ValueError with a descriptive message on bad input")


if __name__ == "__main__":
    main()
