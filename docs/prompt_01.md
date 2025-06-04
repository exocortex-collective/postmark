You are creating the foundation of the *Postmark* email datastore.

**Context**
- Shell project Git repo.
- Python 3.10+.
- Use `uv` for dependency management.

**Task**
1. Update `pyproject.toml` configured for an installable package called `postmark` with sub‑modules `storage`, `email`, `sync`, `cli`.
2. Add dev dependencies: `pytest`, `black`, `isort`, `mypy`, `pre‑commit`.
3. Write `.pre‑commit-config.yaml` that runs black, isort, mypy, and pytest.
4. Add a placeholder `postmark/__init__.py` with `__version__ = "0.0.0"`.

**Tests**
- Running `pytest -q` should exit 0 (no tests yet).
- `pre‑commit run --all-files` should pass.

Write the code and the tests.
