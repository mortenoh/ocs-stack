"""Rewrite links that point outside ``docs/`` to absolute GitHub URLs.

The project pages link to example and library source with paths relative to
the repository root, because that is how these files are usually read: in an
editor, or as markdown on GitHub. The published site is the one place those
paths cannot work -- it contains ``docs/`` and nothing else, so every source
link 404s there.

Rewriting at build time keeps both. The markdown on disk stays relative, so
``scripts/check-links.sh`` can still check it against the filesystem, and the
built site gets a URL that resolves.

Registered as a mkdocs hook in ``mkdocs.yml``; it needs no dependencies beyond
the standard library.
"""

from __future__ import annotations

import posixpath
import re
from typing import Any

REPO = "https://github.com/mortenoh/ocs-stack"
BRANCH = "main"

# Inline markdown links only: [text](target). Reference-style links are not
# used anywhere in docs/, and check-links.sh makes the same assumption.
LINK = re.compile(r"\]\(([^)\s]+)(\s+\"[^\"]*\")?\)")


def _rewrite(target: str, page_dir: str) -> str:
    """Return the URL a target should have on the built site.

    Args:
        target: The link target as written in the markdown.
        page_dir: The page's directory, relative to the repository root.

    Returns:
        The original target, or an absolute GitHub URL when it points outside
        ``docs/``.
    """
    if target.startswith(("http://", "https://", "mailto:", "#", "/")):
        return target

    path, _, anchor = target.partition("#")
    if not path:
        return target

    resolved = posixpath.normpath(posixpath.join(page_dir, path))
    if resolved.startswith("docs/"):
        return target

    # A directory link reads better as the tree view than as a blob.
    view = "tree" if path.endswith("/") else "blob"
    url = f"{REPO}/{view}/{BRANCH}/{resolved}"
    return f"{url}#{anchor}" if anchor else url


def on_page_markdown(markdown: str, page: Any, config: Any, files: Any) -> str:
    """Rewrite outside-docs links on one page.

    Args:
        markdown: The page source.
        page: The mkdocs page, used for its path within ``docs/``.
        config: The mkdocs config (unused).
        files: The mkdocs file collection (unused).

    Returns:
        The page source with outside-docs links made absolute.
    """
    page_dir = posixpath.join("docs", posixpath.dirname(page.file.src_uri))

    def replace(match: re.Match[str]) -> str:
        title = match.group(2) or ""
        return f"]({_rewrite(match.group(1), page_dir)}{title})"

    return LINK.sub(replace, markdown)
