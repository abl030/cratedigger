"""Evaluate the README's copy-paste NixOS quick-start."""

from __future__ import annotations

import json
import re
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

REPO_ROOT = Path(__file__).resolve().parent.parent
README = REPO_ROOT / "README.md"


def _extract_nix_quick_start(readme: str) -> str:
    section = readme.partition("## Running it (NixOS)")[2]
    if not section:
        raise AssertionError("README has no 'Running it (NixOS)' section")
    section = section.partition("\n## ")[0]
    matches = re.findall(r"```nix\n(?P<body>.*?)\n```", section, re.DOTALL)
    if len(matches) != 1:
        raise AssertionError(
            "README NixOS section must contain exactly one fenced Nix example"
        )
    return matches[0]


def _read_nix_quick_start() -> str:
    return _extract_nix_quick_start(README.read_text(encoding="utf-8"))


class TestReadmeNixQuickStart(unittest.TestCase):
    def test_extractor_rejects_a_nix_fence_only_in_a_later_section(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "exactly one fenced Nix example",
        ):
            _extract_nix_quick_start(
                "## Running it (NixOS)\nNo example.\n"
                "## Later\n```nix\n{ outputs = _: {}; }\n```\n"
            )

    def test_extractor_rejects_multiple_nix_fences_in_section(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "exactly one fenced Nix example",
        ):
            _extract_nix_quick_start(
                "## Running it (NixOS)\n"
                "```nix\n{ outputs = _: {}; }\n```\n"
                "```nix\n{ outputs = _: {}; }\n```\n"
                "## Later\n"
            )

    def test_evaluates_as_non_root_basic_auth_install(self) -> None:
        with TemporaryDirectory() as temp_dir:
            Path(temp_dir, "flake.nix").write_text(
                _read_nix_quick_start(),
                encoding="utf-8",
            )
            projection = r"""
              config:
              let
                service = config.services.cratedigger;
                accessGroup = service.web.accessGroup;
                failures = map (assertion: assertion.message)
                  (builtins.filter
                    (assertion:
                      !assertion.assertion
                      && builtins.match
                        "services[.]cratedigger.*"
                        assertion.message != null)
                    config.assertions);
              in {
                inherit failures;
                user = service.user;
                group = service.group;
                isSystemUser = config.users.users.${service.user}.isSystemUser;
                hostName = service.web.hostName;
                gatewayPort = service.web.gatewayPort;
                inherit accessGroup;
                accessGroupDeclared =
                  builtins.hasAttr accessGroup config.users.groups;
                serviceHasAccess =
                  builtins.elem accessGroup
                    config.users.users.${service.user}.extraGroups;
                basicAuthFile = service.web.basicAuthFile;
                insecure = service.web.enableInsecure;
              }
            """
            result = subprocess.run(
                [
                    "nix",
                    "eval",
                    "--impure",
                    "--json",
                    "--no-write-lock-file",
                    "--override-input",
                    "cratedigger",
                    f"path:{REPO_ROOT}",
                    "--apply",
                    projection,
                    f"path:{temp_dir}#nixosConfigurations.myhost.config",
                ],
                capture_output=True,
                check=False,
                text=True,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            json.loads(result.stdout),
            {
                "failures": [],
                "user": "cratedigger",
                "group": "users",
                "isSystemUser": True,
                "hostName": "music.example.net",
                "gatewayPort": 8086,
                "accessGroup": "cratedigger-web",
                "accessGroupDeclared": True,
                "serviceHasAccess": True,
                "basicAuthFile": "/run/secrets/cratedigger.htpasswd",
                "insecure": False,
            },
        )


if __name__ == "__main__":
    unittest.main()
