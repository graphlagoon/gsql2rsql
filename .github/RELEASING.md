# Releasing gsql2rsql

## Quick Reference

### Commit Types → Version Bumps

```bash
feat: new feature           → 0.1.0 → 0.2.0 (minor)
fix: bug fix               → 0.1.0 → 0.1.1 (patch)
perf: performance          → 0.1.0 → 0.1.1 (patch)
docs: documentation        → No bump
test: tests                → No bump
chore: maintenance         → No bump
```

### Breaking Changes → Major Version

```bash
feat!: breaking change     → 0.1.0 → 1.0.0 (major)

# Or with footer
feat: new API design

BREAKING CHANGE: Old API removed
```

## How Releases Work

1. **Merge PR to main** with conventional commit message
2. **GitHub Actions automatically**:
   - Analyzes commits since last release
   - Bumps version
   - Generates CHANGELOG
   - Creates GitHub Release
   - Publishes to PyPI
   - Deploys documentation

## Makefile Commands

```bash
make commit-feat       # Interactive feature commit
make commit-fix        # Interactive fix commit
make release-dry-run   # Preview next release
make changelog         # Generate changelog preview
```

## Manual Release (Emergency Only)

```bash
make release  # Creates release locally
make publish  # Publishes to PyPI
```

**Note**: Manual releases are discouraged. Use GitHub Actions.

## Setup PyPI Trusted Publisher

1. Go to https://pypi.org/manage/account/publishing/
2. Add trusted publisher:
   - Repository: `graphlagoon/gsql2rsql`
   - Workflow: `release.yml`
   - Environment: (empty)

No API tokens needed!

## Full Documentation

See [docs/development/RELEASE_PROCESS.md](../docs/development/RELEASE_PROCESS.md)
