#!/usr/bin/env python3
"""Enforce the documentation standard that CLAUDE.md describes in prose.

Every rule here restates one the repository already claims to follow. They
held when this script was written; the point is that they keep holding, since
a lapse is invisible -- a page is never "broken", only quietly thinner than it
promises to be.

Checks, per project:

1. Every example follows the template: a module docstring carrying What, Why
   and Run, at least one ``# SECTION`` banner, and a ``=== Summary ===`` block.
2. Every example has its own section in the project page, at least
   MIN_SECTION_LINES long -- the floor CLAUDE.md sets for covering an example
   "in depth".
3. Every example is linked from the project page, so the reader can get to the
   source.
4. The ROADMAP and the examples directory agree in both directions.

Usage: scripts/check-docs.py [--min-lines N]

Two parsing details are load-bearing, because both were got wrong first:
headings are found outside fenced code blocks only (a Python comment reads as
a heading otherwise), and example numbers are matched without a trailing word
boundary (``0101_connect`` has none, since ``_`` is a word character).
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
MIN_SECTION_LINES = 80

Heading = tuple[int, int, str]


def projects() -> list[str]:
    """Return every project directory, in the order the nav lists them.

    Returns:
        Project directory names.
    """
    return [
        p.name
        for p in sorted(ROOT.iterdir())
        if (p / "pyproject.toml").is_file() and (p / "examples").is_dir()
    ]


def headings(lines: list[str]) -> list[Heading]:
    """Find markdown headings, ignoring anything inside a fenced code block.

    A line like ``# Version 1: the array itself`` inside a ```python fence is a
    comment, not a heading. Missing that turns a 98-line section into a 10-line
    one and invents a failure.

    Args:
        lines: The page, split into lines.

    Returns:
        One ``(index, level, text)`` per real heading.
    """
    found: list[Heading] = []
    in_fence = False
    for i, line in enumerate(lines):
        if re.match(r"^\s*```", line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        m = re.match(r"^(#{1,6}) (.*)", line)
        if m:
            found.append((i, len(m.group(1)), m.group(2)))
    return found


def check_example_template(path: pathlib.Path) -> list[str]:
    """Check one example against the per-example template.

    Args:
        path: The example file.

    Returns:
        One message per violation; empty when the example conforms.
    """
    text = path.read_text(encoding="utf-8")
    doc = re.match(r'^"""(.*?)"""', text, re.S)
    problems: list[str] = []
    if not doc:
        return [f"{path}: no module docstring"]
    header = doc.group(1)
    for key in ("What", "Why", "Run"):
        if f"{key}:" not in header:
            problems.append(f"{path}: module docstring has no '{key}:' line")
    # Banners sit inside main(), so they are indented.
    if not re.search(r"^\s*# SECTION", text, re.M):
        problems.append(f"{path}: no '# SECTION' banner comments")
    if "=== Summary ===" not in text:
        problems.append(f"{path}: no '=== Summary ===' block")
    return problems


def check_project(project: str, min_lines: int) -> list[str]:
    """Check one project's examples, page, and roadmap against each other.

    Args:
        project: Project directory name.
        min_lines: Floor for a per-example documentation section.

    Returns:
        One message per violation; empty when the project conforms.
    """
    problems: list[str] = []
    examples = sorted((ROOT / project / "examples").glob("*.py"))
    if not examples:
        return [f"{project}: no examples found"]

    for path in examples:
        problems += check_example_template(path)

    page_path = ROOT / "docs" / "projects" / f"{project}.md"
    if not page_path.is_file():
        return problems + [f"{project}: no page at docs/projects/{project}.md"]

    page = page_path.read_text(encoding="utf-8")
    lines = page.split("\n")
    heads = headings(lines)

    # Map each example number to the span of its documentation section.
    spans: dict[str, int] = {}
    for i, level, text in heads:
        m = re.match(r"[`']?(\d{4})", text)
        if not m:
            continue
        following = [j for j, lv, _ in heads if j > i and lv <= level]
        spans[m.group(1)] = (following[0] if following else len(lines)) - i

    for path in examples:
        number = path.stem[:4]
        if number not in spans:
            problems.append(f"{project}: {path.name} has no section in the project page")
        elif spans[number] < min_lines:
            problems.append(
                f"{project}: the section for {path.name} is {spans[number]} lines, "
                f"under the {min_lines}-line floor"
            )
        if path.name not in page:
            problems.append(f"{project}: the project page never links {path.name}")

    roadmap_path = ROOT / project / "ROADMAP.md"
    if roadmap_path.is_file():
        roadmap = roadmap_path.read_text(encoding="utf-8")
        names = {p.stem for p in examples}
        for path in examples:
            if path.stem not in roadmap:
                problems.append(f"{project}: {path.name} is missing from ROADMAP.md")
        for named in set(re.findall(r"\b\d{4}_[a-z0-9_]+", roadmap)):
            if named not in names:
                problems.append(f"{project}: ROADMAP.md names {named}, which does not exist")
    else:
        problems.append(f"{project}: no ROADMAP.md")

    return problems


def main() -> int:
    """Run every check and report.

    Returns:
        0 when everything conforms, 1 otherwise.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-lines", type=int, default=MIN_SECTION_LINES)
    args = parser.parse_args()

    problems: list[str] = []
    total = 0
    for project in projects():
        total += len(list((ROOT / project / "examples").glob("*.py")))
        problems += check_project(project, args.min_lines)

    for problem in problems:
        print(f"  {problem}")

    print()
    if problems:
        print(f"{len(problems)} documentation problem(s) across {total} examples.")
        return 1
    print(f"All {total} examples follow the template and are documented in depth.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
