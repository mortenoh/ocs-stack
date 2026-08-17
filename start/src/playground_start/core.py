"""Small, well-tested functions used to exercise the project template."""

import re


def fizzbuzz(n: int) -> list[str]:
    """Return the fizzbuzz sequence from 1 to n inclusive.

    Args:
        n: Upper bound of the sequence; must be at least 1.

    Returns:
        A list where multiples of 3 are "fizz", multiples of 5 are "buzz",
        multiples of both are "fizzbuzz", and everything else is the number
        as a string.

    Raises:
        ValueError: If n is less than 1.
    """
    if n < 1:
        raise ValueError(f"n must be at least 1, got {n}")
    out: list[str] = []
    for i in range(1, n + 1):
        match (i % 3, i % 5):
            case (0, 0):
                out.append("fizzbuzz")
            case (0, _):
                out.append("fizz")
            case (_, 0):
                out.append("buzz")
            case _:
                out.append(str(i))
    return out


def slugify(text: str) -> str:
    """Turn arbitrary text into a lowercase, hyphen-separated slug.

    Args:
        text: The text to slugify; must contain at least one alphanumeric
            character.

    Returns:
        The slug: lowercased, with runs of non-alphanumeric characters
        collapsed into single hyphens and no leading or trailing hyphens.

    Raises:
        ValueError: If the text contains no alphanumeric characters.
    """
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    if not slug:
        raise ValueError(f"text has no alphanumeric characters: {text!r}")
    return slug
