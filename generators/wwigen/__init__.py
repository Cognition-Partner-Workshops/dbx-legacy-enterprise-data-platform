"""wwigen - the deterministic generator for the legacy estate's source data.

The package is standard library only and writes files, never database rows.
See ``generators/README.md`` for how the pieces fit together.
"""

__all__ = [
    "config", "context", "defects", "documents", "entities", "keys",
    "manifest", "regions", "rng", "schema", "skew", "tables", "text",
    "timeline", "writers",
]
