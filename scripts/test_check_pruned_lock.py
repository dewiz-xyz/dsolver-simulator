#!/usr/bin/env python3
"""Exercise lock provenance and dependency-edge checks without external packages."""

import copy
import json
import tempfile
import unittest
from pathlib import Path

import check_pruned_lock as GUARD

REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
PRIVATE = {
    "name": "tycho-execution",
    "version": "0.165.1",
    "source": "git+ssh://git@github.com/pedrobergamini/dsolver-execution.git#46596c8",
    "dependencies": [],
}
PUBLIC = [
    {
        "name": "middle",
        "version": "1.0.0",
        "source": REGISTRY,
        "checksum": "a" * 64,
        "dependencies": ["dep 1.0.0"],
    },
    {
        "name": "dep",
        "version": "1.0.0",
        "source": REGISTRY,
        "checksum": "b" * 64,
        "dependencies": [],
    },
    {
        "name": "dep",
        "version": "2.0.0",
        "source": REGISTRY,
        "checksum": "c" * 64,
        "dependencies": [],
    },
]


def write_lock(path, packages):
    lines = ["version = 4", ""]
    for package in packages:
        lines.append("[[package]]")
        for field in ["name", "version", "source", "checksum", "dependencies"]:
            if field in package:
                lines.append(f"{field} = {json.dumps(package[field])}")
        lines.append("")
    path.write_text("\n".join(lines))


class PrunedLockTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.original = Path(self.directory.name) / "original.lock"
        self.pruned = Path(self.directory.name) / "pruned.lock"
        write_lock(self.original, PUBLIC + [PRIVATE])

    def reject(self, packages, message):
        write_lock(self.pruned, packages)
        with self.assertRaisesRegex(ValueError, message):
            GUARD.check(self.original, self.pruned)

    def test_altered_version_source_and_checksum_are_rejected(self):
        changes = {
            "version": "9.0.0",
            "source": "registry+https://other.example/index",
            "checksum": "f" * 64,
        }
        for field, replacement in changes.items():
            with self.subTest(field=field):
                packages = copy.deepcopy(PUBLIC)
                packages[0][field] = replacement
                self.reject(packages, "changed package identities")

    def test_external_source_cannot_be_removed(self):
        packages = copy.deepcopy(PUBLIC)
        del packages[0]["source"]
        self.reject(packages, "changed package identities")

    def test_new_external_package_is_rejected(self):
        packages = copy.deepcopy(PUBLIC)
        added = copy.deepcopy(PUBLIC[0])
        added["name"] = "new-package"
        self.reject(packages + [added], "changed package identities")

    def test_added_dependency_edge_is_rejected(self):
        packages = copy.deepcopy(PUBLIC)
        packages[0]["dependencies"].append("dep 2.0.0")
        self.reject(packages, "added or retargeted dependencies")

    def test_edge_cannot_retarget_an_already_locked_version(self):
        packages = copy.deepcopy(PUBLIC)
        packages[0]["dependencies"] = ["dep 2.0.0"]
        self.reject(packages, "added or retargeted dependencies")

    def test_private_execution_dependency_is_rejected(self):
        self.reject(PUBLIC + [PRIVATE], "private execution repository")

    def test_workspace_edge_cannot_retarget_an_already_locked_version(self):
        workspace = {
            "name": "historical-quote-service",
            "version": "0.1.0",
            "dependencies": ["dep 1.0.0"],
        }
        write_lock(self.original, PUBLIC + [PRIVATE, workspace])
        workspace["dependencies"] = ["dep 2.0.0"]
        self.reject(PUBLIC + [workspace], "added or retargeted dependencies")

    def test_package_and_edge_removals_are_allowed(self):
        packages = copy.deepcopy(PUBLIC[:2])
        packages[0]["dependencies"] = []
        write_lock(self.pruned, packages)
        GUARD.check(self.original, self.pruned)

    def test_identical_public_lock_is_allowed(self):
        write_lock(self.original, PUBLIC)
        write_lock(self.pruned, PUBLIC)
        GUARD.check(self.original, self.pruned)


if __name__ == "__main__":
    unittest.main(verbosity=2)
