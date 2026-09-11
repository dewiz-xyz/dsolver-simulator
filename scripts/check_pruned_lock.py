#!/usr/bin/env python3
"""Check that workspace pruning only removes locked packages and dependency edges."""

import argparse
import tomllib
from pathlib import Path


def package_records(path):
    packages = tomllib.loads(path.read_text())["package"]
    return {
        (
            package["name"],
            package["version"],
            package.get("source"),
            package.get("checksum"),
        ): set(package.get("dependencies", []))
        for package in packages
    }


def check(original, pruned):
    original_records = package_records(original)
    pruned_records = package_records(pruned)
    introduced = pruned_records.keys() - original_records.keys()
    if introduced:
        raise ValueError(
            "Pruned lock introduced or changed package identities: "
            + repr(sorted(introduced, key=repr))
        )

    for identity, dependencies in pruned_records.items():
        added_edges = dependencies - original_records[identity]
        if added_edges:
            raise ValueError(
                f"Pruned lock added or retargeted dependencies for "
                f"{identity[0]} {identity[1]}: {sorted(added_edges)}"
            )
        if identity[2] and "pedrobergamini/dsolver-execution" in identity[2]:
            raise ValueError(
                "Historical workspace still resolves the private execution repository"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("original", type=Path)
    parser.add_argument("pruned", type=Path)
    args = parser.parse_args()
    check(args.original, args.pruned)
