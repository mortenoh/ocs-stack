# playground-lang-start

Starter project verifying the playground-python template: uv, src layout,
ruff, mypy, pyright, pytest, coverage, and mkdocs-material with mkdocstrings.

## Quick start

```bash
make install     # uv sync
make lint        # ruff format + check, mypy, pyright
make test        # pytest
make run-all     # run every example
```

## Layout

```text
lang/start/
  pyproject.toml              uv_build backend, all tool config
  src/playground_lang_start/  the library (src layout)
  tests/                      pytest tests
  examples/                   numbered runnable lessons
  docs/ + mkdocs.yml          this site
```

See the [API Reference](api-reference.md) for the library itself.
