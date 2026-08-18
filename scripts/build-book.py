#!/usr/bin/env python3
"""Render the whole documentation set as one self-contained HTML file.

The mkdocs site is the right way to read these pages at a desk: many files,
a sidebar, live search. On a phone, on a train, with no connection, none of
that helps. This script produces the other shape -- a single file, no assets,
no network, that a browser opens instantly and keeps working offline.

It reads the nav order out of ``mkdocs.yml`` so the book and the site cannot
drift, converts each page with the same markdown extensions the site uses, and
resolves the cross-page links into intra-document anchors.

Usage:
    scripts/build-book.py [--out dist/climate-stack.html]

Run it through ``make book`` (or ``make pdf``), which supplies the toolchain
via ``uvx`` -- there is no root virtualenv and this script needs none.
"""

from __future__ import annotations

import argparse
import html
import os
import re
import sys
from pathlib import Path
from typing import Any

import markdown
import yaml

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"

# The same set the site builds with, minus the mermaid superfence: a mermaid
# block has no renderer in a standalone file, so it is left as a plain fenced
# block rather than emitting an empty div.
EXTENSIONS = [
    "admonition",
    "attr_list",
    "def_list",
    "footnotes",
    "tables",
    "toc",
    "pymdownx.details",
    "pymdownx.highlight",
    "pymdownx.inlinehilite",
    "pymdownx.superfences",
    "pymdownx.tabbed",
    "pymdownx.tasklist",
]

EXTENSION_CONFIGS: dict[str, dict[str, Any]] = {
    "pymdownx.highlight": {"anchor_linenums": True},
    "pymdownx.tabbed": {"alternate_style": True},
    "pymdownx.tasklist": {"custom_checkbox": True},
    "toc": {"permalink": False},
}


class NavEntry:
    """One page in the book, with the slug its anchors are namespaced under."""

    def __init__(self, title: str, path: str, part: str) -> None:
        self.title = title
        self.path = path
        self.part = part
        # docs/projects/xarray.md -> projects-xarray, which prefixes every
        # heading id on the page so two pages can both have "## Setup".
        self.slug = path[: -len(".md")].replace("/", "-")

    @property
    def file(self) -> Path:
        """Absolute path to the source markdown."""
        return DOCS / self.path


def read_nav(config_path: Path) -> list[NavEntry]:
    """Extract the ordered page list from a mkdocs config.

    Args:
        config_path: Path to ``mkdocs.yml``.

    Returns:
        Every page in nav order, each tagged with the section it sits under.

    Raises:
        ValueError: If the config has no ``nav`` key.
    """
    # mkdocs.yml uses a `!!python/name:` tag for the mermaid fence, which the
    # safe loader refuses. Nothing here needs that key, so the tag is ignored.
    loader = yaml.SafeLoader
    loader.add_multi_constructor("tag:yaml.org,2002:python/name:", lambda *_: None)
    config = yaml.load(config_path.read_text(encoding="utf-8"), Loader=loader)  # noqa: S506

    nav = config.get("nav")
    if not nav:
        raise ValueError(f"{config_path} has no nav section")

    entries: list[NavEntry] = []

    def walk(items: list[Any], part: str) -> None:
        for item in items:
            for title, target in item.items():
                if isinstance(target, str):
                    entries.append(NavEntry(title, target, part))
                else:
                    walk(target, title)

    walk(nav, "")
    return entries


def rewrite_links(body: str, entry: NavEntry, by_path: dict[str, NavEntry]) -> str:
    """Point cross-page links at in-document anchors and defuse source links.

    Three kinds of relative link appear in these pages: to another page
    (``stack.md``, ``../projects/dask.md#chunking``), to a heading on the same
    page (``#setup``), and to a source file outside ``docs/``
    (``../../xarray/examples/0101_x.py``). The first two can be resolved into
    this document; the third cannot exist in a standalone file, so it becomes
    the path in monospace -- still the information the sentence needed.

    Args:
        body: Rendered HTML for one page.
        entry: The page being rewritten, used to resolve relative paths.
        by_path: Every page in the book, keyed by its docs-relative path.

    Returns:
        The HTML with every relative href resolved.
    """
    here = Path(entry.path).parent

    def resolve(match: re.Match[str]) -> str:
        href = match.group(1)
        if re.match(r"^(https?:|mailto:)", href):
            return match.group(0)

        if href.startswith("#"):
            return f'href="#{entry.slug}--{href[1:]}"'

        target, _, anchor = href.partition("#")
        # Normalise ../ against the page's own directory.
        resolved = str((here / target).as_posix())
        while "/../" in resolved or resolved.startswith("../"):
            resolved = re.sub(r"(^|/)[^/]+/\.\./", r"\1", resolved, count=1)
            if resolved.startswith("../"):
                break

        page = by_path.get(resolved)
        if page is None:
            return f'href="SOURCE:{href}"'  # marked for the second pass below
        if anchor:
            return f'href="#{page.slug}--{anchor}"'
        return f'href="#{page.slug}"'

    body = re.sub(r'href="([^"]+)"', resolve, body)

    # Second pass: an unresolvable link was a path to source outside docs/.
    # Replace the whole anchor element with the path as inline code.
    def strip_anchor(match: re.Match[str]) -> str:
        # These are written relative to docs/projects/, so drop the leading
        # ../ hops and show the path as it reads from the repository root.
        target = re.sub(r"^(\.\./)+", "", match.group(1))
        text = match.group(2)
        # Keep the author's link text; append the path only when it adds
        # something the text does not already say.
        bare = re.sub(r"<[^>]+>", "", text).strip()
        if bare and bare in target:
            return f"<code>{html.escape(target)}</code>"
        return f"{text} (<code>{html.escape(target)}</code>)"

    return re.sub(r'<a href="SOURCE:([^"]*)"[^>]*>(.*?)</a>', strip_anchor, body, flags=re.DOTALL)


def namespace_ids(body: str, slug: str) -> str:
    """Prefix every id on a page so ids stay unique across the book.

    Every id, not only headings: the markdown extensions mint their own, and
    they are per-page counters that collide the moment two pages use the same
    feature. Two pages with one footnote each both emit ``fn:1`` and
    ``fnref:1``, so every footnote link in the book would jump to the first
    page's note. The same goes for ``__codelineno-`` anchors from
    ``anchor_linenums`` and ``__tabbed_N_N`` from the tabbed extension.

    :func:`rewrite_links` prefixes same-page ``href="#..."`` targets with the
    same slug, so the two stay in step as long as both cover the same ids.

    Args:
        body: Rendered HTML for one page.
        slug: The page's slug.

    Returns:
        The HTML with every ``id="x"`` rewritten to ``id="slug--x"``.
    """
    return re.sub(r'(\sid=")([^"]+)(")', rf"\1{slug}--\2\3", body)


def demote_headings(body: str) -> str:
    """Shift every heading down one level so page titles nest under parts.

    The book has one ``<h1>`` per page already; wrapping them in parts would
    give two competing top levels. Demoting keeps the outline honest, and h6
    stays h6 rather than overflowing into an invalid h7.

    Args:
        body: Rendered HTML for one page.

    Returns:
        The HTML with headings demoted by one level.
    """
    for level in range(5, 0, -1):
        body = re.sub(rf"<h{level}([ >])", rf"<h{level + 1}\1", body)
        body = body.replace(f"</h{level}>", f"</h{level + 1}>")
    return body


CSS = """
:root {
  --bg: #ffffff; --fg: #1a1c1f; --muted: #5b6169; --rule: #e2e5ea;
  --accent: #2d4f8e; --code-bg: #f5f6f8; --quote: #eef1f6;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #14161a; --fg: #dfe3e8; --muted: #98a0aa; --rule: #2a2e35;
    --accent: #8fb0ea; --code-bg: #1c1f25; --quote: #1a1e26;
  }
}
* { box-sizing: border-box; }
html { -webkit-text-size-adjust: 100%; }
body {
  margin: 0 auto; padding: 1.25rem 1.1rem 6rem; max-width: 44rem;
  background: var(--bg); color: var(--fg);
  font: 400 17px/1.65 -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  overflow-wrap: break-word;
}
h1, h2, h3, h4, h5, h6 { line-height: 1.25; margin: 2.2em 0 0.6em; font-weight: 650; }
h1 { font-size: 1.9rem; margin-top: 0; }
h2 { font-size: 1.5rem; padding-bottom: 0.25rem; border-bottom: 1px solid var(--rule); }
h3 { font-size: 1.22rem; }
h4 { font-size: 1.05rem; color: var(--muted); }
a { color: var(--accent); }
p, li { }
code, pre, kbd { font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, monospace; }
code { background: var(--code-bg); padding: 0.12em 0.34em; border-radius: 4px; font-size: 0.87em; }
pre {
  background: var(--code-bg); padding: 0.85rem 0.95rem; border-radius: 7px;
  overflow-x: auto; font-size: 0.8rem; line-height: 1.5;
}
pre code { background: none; padding: 0; font-size: inherit; }
blockquote {
  margin: 1.2em 0; padding: 0.6em 1em; background: var(--quote);
  border-left: 3px solid var(--accent); border-radius: 0 5px 5px 0;
}
blockquote > :first-child { margin-top: 0; }
blockquote > :last-child { margin-bottom: 0; }
table { border-collapse: collapse; width: 100%; font-size: 0.9rem; }
.table-wrap { overflow-x: auto; margin: 1.2em 0; }
th, td { border: 1px solid var(--rule); padding: 0.42rem 0.6rem; text-align: left; vertical-align: top; }
th { background: var(--code-bg); font-weight: 600; }
.admonition {
  margin: 1.3em 0; padding: 0.75rem 1rem; border-radius: 6px;
  background: var(--quote); border-left: 3px solid var(--accent);
}
.admonition-title { margin: 0 0 0.4em; font-weight: 650; }
.admonition > :last-child { margin-bottom: 0; }
hr { border: 0; border-top: 1px solid var(--rule); margin: 2.5em 0; }
.part {
  margin: 3.5rem 0 0; padding-top: 1.2rem; border-top: 2px solid var(--accent);
  font-size: 0.78rem; letter-spacing: 0.14em; text-transform: uppercase;
  color: var(--accent); font-weight: 700;
}
.page { padding-top: 0.4rem; }
.page + .page { border-top: 1px solid var(--rule); margin-top: 3rem; padding-top: 2.4rem; }
.cover { margin-bottom: 3rem; }
.cover h1 { font-size: 2.3rem; margin-bottom: 0.3rem; }
.cover .sub { color: var(--muted); font-size: 1.02rem; margin-top: 0; }
.toc-part { margin: 1.6em 0 0.4em; font-size: 0.78rem; letter-spacing: 0.12em;
  text-transform: uppercase; color: var(--muted); font-weight: 700; }
.toc { list-style: none; padding: 0; margin: 0; }
.toc li { margin: 0.28em 0; }
.backlink { display: block; margin-top: 1.6rem; font-size: 0.82rem; color: var(--muted); }

@media print {
  :root { --bg: #fff; --fg: #000; --code-bg: #f3f4f6; --quote: #f3f4f6; --rule: #ccc; }
  body { max-width: none; font-size: 10.5pt; padding: 0; }
  .backlink, .no-print { display: none; }
  .page { break-before: page; }
  .cover, .cover + .page { break-before: auto; }
  h1, h2, h3, h4 { break-after: avoid; }
  pre, blockquote, table, .admonition { break-inside: avoid; }
  a { color: inherit; text-decoration: none; }
  @page { size: A4; margin: 16mm 15mm; }
}
"""


def build(out_path: Path) -> int:
    """Render every nav page into one HTML file.

    Args:
        out_path: Where to write the book.

    Returns:
        The number of pages rendered.

    Raises:
        FileNotFoundError: If a page listed in the nav does not exist.
    """
    entries = read_nav(ROOT / "mkdocs.yml")
    by_path = {e.path: e for e in entries}

    missing = [e.path for e in entries if not e.file.is_file()]
    if missing:
        raise FileNotFoundError(f"nav lists pages that do not exist: {', '.join(missing)}")

    converter = markdown.Markdown(extensions=EXTENSIONS, extension_configs=EXTENSION_CONFIGS)

    toc: list[str] = []
    parts: list[str] = []
    current_part = None

    for entry in entries:
        if entry.part != current_part:
            current_part = entry.part
            if current_part:
                toc.append(f'<div class="toc-part">{html.escape(current_part)}</div><ul class="toc">')
                parts.append(f'<div class="part">{html.escape(current_part)}</div>')
            else:
                toc.append('<ul class="toc">')
        toc.append(f'<li><a href="#{entry.slug}">{html.escape(entry.title)}</a></li>')

        converter.reset()
        body = converter.convert(entry.file.read_text(encoding="utf-8"))
        body = namespace_ids(body, entry.slug)
        body = rewrite_links(body, entry, by_path)
        body = demote_headings(body)
        # Wide tables must scroll inside their own box, not push the page.
        body = body.replace("<table>", '<div class="table-wrap"><table>').replace("</table>", "</table></div>")

        parts.append(
            f'<section class="page" id="{entry.slug}">\n'
            f"<h1>{html.escape(entry.title)}</h1>\n{body}\n"
            f'<a class="backlink" href="#contents">Back to contents</a>\n'
            f"</section>"
        )

    toc.append("</ul>")

    document = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>climate-stack</title>
<style>{CSS}</style>
</head>
<body>
<header class="cover">
<h1>climate-stack</h1>
<p class="sub">xarray, dask, dask.distributed, icechunk, and a climate pipeline &mdash;
the whole documentation set as one offline page.</p>
</header>
<nav id="contents">
<h2>Contents</h2>
{"".join(toc)}
</nav>
{"".join(parts)}
</body>
</html>
"""

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(document, encoding="utf-8")
    return len(entries)


def main() -> int:
    """Parse arguments and build the book.

    Returns:
        A process exit code.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=ROOT / "dist" / "climate-stack.html",
        help="output path (default: dist/climate-stack.html)",
    )
    args = parser.parse_args()

    count = build(args.out)
    size_kb = args.out.stat().st_size / 1024
    # relative_to raises for anything outside ROOT, which a caller-supplied
    # --out very often is; os.path.relpath just walks up instead.
    shown = os.path.relpath(args.out, ROOT)
    print(f"wrote {shown} -- {count} pages, {size_kb:.0f} KiB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
