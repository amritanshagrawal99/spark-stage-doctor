# Releasing

Releases go to PyPI from GitHub Actions with [trusted publishing](https://docs.pypi.org/trusted-publishers/): no API token
is stored anywhere.

**One-time setup** (repository owner):
1. On pypi.org, under *Your account → Publishing → Add a new pending publisher*, enter:
   - project `spark-stage-doctor`
   - owner `amritanshagrawal99`
   - repository `spark-stage-doctor`
   - workflow `release.yml`
   - environment `pypi`
2. On GitHub, under *Settings → Environments*, create an environment named `pypi`. Optionally require a reviewer.

**Each release:**
```bash
# bump version in pyproject.toml and src/spark_stage_doctor/__init__.py, commit, then:
git tag v0.1.0 && git push origin v0.1.0
```
The workflow checks that the tag matches the version, builds the sdist and wheel, runs `twine check`, and publishes.
