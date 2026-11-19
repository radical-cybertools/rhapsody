#!/usr/bin/env python3
"""Check the RHAPSODY skills against the Agent Skills specification and this repository.

For every ``skills/<name>/SKILL.md`` this script checks:

* frontmatter: delimited by ``---`` lines, parses to a mapping, has ``name`` and ``description``;
  only spec keys are used (``name``, ``description``, ``metadata`` are portable across clients;
  ``license``, ``compatibility``, ``allowed-tools`` produce a portability warning);
* ``name``: 1-64 chars, lowercase letters/digits/hyphens, no leading/trailing/double hyphen,
  equal to the directory name;
* ``description``: 1-1024 chars and says when to use the skill;
* ``metadata``: a mapping of string keys to string values;
* size: SKILL.md under 500 lines and roughly 5000 tokens (warnings);
* links: relative Markdown links resolve inside the skill directory (no ``../`` or absolute paths);
* ``$RHAPSODY_REPO/<path>`` references exist in this repository (so the skill never points at
  files that were moved or deleted).

Usage::

    python skills/tools/check_skills.py            # all skills
    python skills/tools/check_skills.py rhapsody-execution

Exit status 1 if any error was found. Requires PyYAML.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover
    sys.exit("check_skills.py needs PyYAML: pip install pyyaml")

SKILLS_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = SKILLS_DIR.parent
SPEC_KEYS = {"name", "description", "license", "compatibility", "metadata", "allowed-tools"}
PORTABLE_KEYS = {"name", "description", "metadata"}
NAME_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
LINK_RE = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
REPO_REF_RE = re.compile(r"\$RHAPSODY_REPO/([\w./-]+)")
URL_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")


def check_skill(skill_dir: Path) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    path = skill_dir / "SKILL.md"
    text = path.read_text(encoding="utf-8")
    lines = text.split("\n")

    if lines[0] != "---":
        return [f"{path}: first line must be '---'"], warnings
    try:
        end = lines.index("---", 1)
    except ValueError:
        return [f"{path}: missing closing '---'"], warnings
    try:
        fm = yaml.safe_load("\n".join(lines[1:end]))
    except yaml.YAMLError as exc:
        return [f"{path}: frontmatter is not valid YAML: {exc}"], warnings
    if not isinstance(fm, dict):
        return [f"{path}: frontmatter must be a mapping"], warnings

    for key in fm:
        if key not in SPEC_KEYS:
            errors.append(f"{path}: unknown frontmatter key {key!r}")
        elif key not in PORTABLE_KEYS:
            warnings.append(f"{path}: key {key!r} is not honoured by every client")

    name = fm.get("name")
    if not isinstance(name, str) or not name:
        errors.append(f"{path}: 'name' is required")
    else:
        if len(name) > 64 or not NAME_RE.match(name):
            errors.append(
                f"{path}: name {name!r} must be 1-64 chars of [a-z0-9-], no edge/double '-'"
            )
        if name != skill_dir.name:
            errors.append(f"{path}: name {name!r} must equal the directory name {skill_dir.name!r}")

    desc = fm.get("description")
    if not isinstance(desc, str) or not desc.strip():
        errors.append(f"{path}: 'description' is required")
    else:
        if len(desc) > 1024:
            errors.append(f"{path}: description is {len(desc)} chars (max 1024)")
        if not re.search(r"\b(use|when)\b", desc, re.IGNORECASE):
            warnings.append(f"{path}: description should say when to use the skill")

    meta = fm.get("metadata")
    if meta is not None and (
        not isinstance(meta, dict)
        or not all(isinstance(k, str) and isinstance(v, str) for k, v in meta.items())
    ):
        errors.append(f"{path}: metadata must map strings to strings (quote numbers)")

    body = "\n".join(lines[end + 1 :])
    if len(lines) > 500:
        warnings.append(f"{path}: {len(lines)} lines (keep SKILL.md under 500)")
    if len(body) / 4 > 5000:
        warnings.append(f"{path}: body is ~{len(body) // 4} tokens (keep under ~5000)")

    for md in sorted(skill_dir.rglob("*.md")):
        md_text = md.read_text(encoding="utf-8")
        rel = md.relative_to(SKILLS_DIR)
        for target in LINK_RE.findall(md_text):
            if URL_RE.match(target) or target.startswith("#"):
                continue
            target = target.split("#", 1)[0]
            if target.startswith(("/", "~")) or ".." in Path(target).parts:
                errors.append(f"{rel}: link {target!r} leaves the skill directory")
            elif not (md.parent / target).exists():
                errors.append(f"{rel}: link {target!r} does not exist")
        for ref in REPO_REF_RE.findall(md_text):
            ref = ref.rstrip(".,)")
            if not (REPO_ROOT / ref).exists():
                errors.append(f"{rel}: $RHAPSODY_REPO/{ref} does not exist in this repository")
    return errors, warnings


def main() -> int:
    wanted = set(sys.argv[1:])
    skills = sorted(
        d
        for d in SKILLS_DIR.iterdir()
        if (d / "SKILL.md").is_file() and (not wanted or d.name in wanted)
    )
    if not skills:
        print("no skills found", file=sys.stderr)
        return 1
    n_err = 0
    for skill in skills:
        errors, warnings = check_skill(skill)
        n_err += len(errors)
        status = "FAIL" if errors else "ok"
        print(f"{status:<5}{skill.name}")
        for e in errors:
            print(f"  error:   {e}")
        for w in warnings:
            print(f"  warning: {w}")
    return 1 if n_err else 0


if __name__ == "__main__":
    sys.exit(main())
