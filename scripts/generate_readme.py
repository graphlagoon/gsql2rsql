#!/usr/bin/env python3
"""Generate README.md from docs/index.md with compiled SQL examples.

This script renders the Jinja2 macros in docs/index.md and outputs
a GitHub-compatible README.md with actual SQL examples.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "docs"))

from macros import fraud_example_sql, simple_match_sql  # noqa: E402


def render_macros(content: str) -> str:
    """Replace Jinja2 macro calls with their rendered output.

    For README, we render fraud_example_sql with include_fence=True
    (to include the ```sql``` markers) but without indent (since GitHub
    <details> tags don't need indented content).
    """
    # Match macro calls with any arguments: {{ func(...) }}
    # For fraud_example_sql, we call with include_fence=True for README
    pattern = r"\{\{\s*fraud_example_sql\([^)]*\)\s*\}\}"
    if re.search(pattern, content):
        result = fraud_example_sql(indent=0, include_fence=True)
        content = re.sub(pattern, result, content)

    # simple_match_sql has no special parameters
    pattern = r"\{\{\s*simple_match_sql\([^)]*\)\s*\}\}"
    if re.search(pattern, content):
        result = simple_match_sql()
        content = re.sub(pattern, result, content)

    return content


def clean_mkdocs_syntax(content: str) -> str:
    """Remove/convert MkDocs-specific syntax for GitHub README."""
    lines = content.split("\n")
    result = []
    in_admonition = False
    in_collapsible = False
    in_code_block = False

    i = 0
    while i < len(lines):
        line = lines[i]

        # Track code blocks (they should be passed through as-is)
        if line.strip().startswith("```"):
            if in_code_block:
                in_code_block = False
            else:
                in_code_block = True
            # If in collapsible, de-indent
            if in_collapsible and line.startswith("    "):
                result.append(line[4:])
            else:
                result.append(line)
            i += 1
            continue

        # Inside code block - pass through (de-indent if in collapsible)
        if in_code_block:
            if in_collapsible and line.startswith("    "):
                result.append(line[4:])
            else:
                result.append(line)
            i += 1
            continue

        # Handle collapsible blocks: ??? type "title"
        match = re.match(r'^\?\?\?\+?\s+\w+\s+"([^"]+)"', line)
        if match:
            title = match.group(1)
            result.append("<details>")
            result.append(f"<summary>{title}</summary>")
            result.append("")
            in_collapsible = True
            i += 1
            continue

        # Handle admonitions: !!! type "title" or !!! type
        match = re.match(r'^!!!\s+(\w+)(?:\s+"([^"]+)")?', line)
        if match:
            title = match.group(2) or match.group(1).title()
            result.append(f"> **{title}**")
            result.append(">")
            in_admonition = True
            i += 1
            continue

        # Handle indented content in collapsible/admonition
        if (in_collapsible or in_admonition) and line.startswith("    "):
            content_line = line[4:]  # Remove 4-space indent
            if in_admonition:
                result.append(f"> {content_line}")
            else:
                result.append(content_line)
            i += 1
            continue

        # Handle empty lines in blocks
        if (in_collapsible or in_admonition) and line.strip() == "":
            if in_admonition:
                result.append(">")
            else:
                result.append("")
            i += 1
            continue

        # End of indented block (only if not in code block)
        if in_collapsible and not line.startswith("    ") and line.strip():
            result.append("")
            result.append("</details>")
            result.append("")
            in_collapsible = False

        if in_admonition and not line.startswith("    ") and line.strip():
            result.append("")
            in_admonition = False

        # Convert tabs: === "title" -> ### title
        match = re.match(r'^===\s+"([^"]+)"', line)
        if match:
            result.append(f"### {match.group(1)}")
            i += 1
            continue

        # Remove snippet includes
        if line.strip().startswith("--8<--"):
            i += 1
            continue

        result.append(line)
        i += 1

    # Close any unclosed blocks
    if in_collapsible:
        result.append("")
        result.append("</details>")

    content = "\n".join(result)

    # Clean up multiple blank lines
    content = re.sub(r"\n{3,}", "\n\n", content)

    return content


def add_readme_header(content: str) -> str:
    """Add README-specific header with badges."""
    badges = """
[![PyPI version](https://badge.fury.io/py/gsql2rsql.svg)](https://badge.fury.io/py/gsql2rsql)
[![CI](https://github.com/graphlagoon/gsql2rsql/actions/workflows/ci.yml/badge.svg)](https://github.com/graphlagoon/gsql2rsql/actions/workflows/ci.yml)
[![Documentation](https://img.shields.io/badge/docs-mkdocs-blue)](https://graphlagoon.github.io/gsql2rsql)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

"""
    lines = content.split("\n")
    for i, line in enumerate(lines):
        if line.startswith("# "):
            lines.insert(i + 1, badges)
            break

    return "\n".join(lines)


def main():
    """Generate README.md from docs/index.md."""
    docs_index = PROJECT_ROOT / "docs" / "index.md"
    readme = PROJECT_ROOT / "README.md"

    if not docs_index.exists():
        print(f"Error: {docs_index} not found")
        sys.exit(1)

    print(f"Reading {docs_index}...")
    content = docs_index.read_text()

    print("Rendering macros...")
    content = render_macros(content)

    print("Converting MkDocs syntax...")
    content = clean_mkdocs_syntax(content)

    print("Adding README header...")
    content = add_readme_header(content)

    print(f"Writing {readme}...")
    readme.write_text(content)

    print("Done!")


if __name__ == "__main__":
    main()
