# Mutation-testing shell — mutmut + pytest layered onto the same
# flake-locked test environment as shell.nix, for the implementer-side
# catalog breadth pass (docs/mutation-testing.md, issue #1317).
#
#   nix-shell nix/mutmut-shell.nix --run "mutmut run"
#
# Kept separate from shell.nix deliberately: the canonical dev shell is
# the pinned real-beets contract environment and the suite substrate;
# mutation tooling has no business in its closure.
{ pkgs ? import (
    let
      lock = builtins.fromJSON (builtins.readFile ../flake.lock);
      node = lock.nodes.${lock.nodes.${lock.root}.inputs.nixpkgs}.locked;
    in
    builtins.fetchTarball {
      url = "https://github.com/${node.owner}/${node.repo}/archive/${node.rev}.tar.gz";
      sha256 = node.narHash;
    }
  ) {} }:

let
  beetsPackage = import ./beets.nix { inherit pkgs; };
  cratedigger = import ./package.nix { inherit pkgs beetsPackage; };

  # nixpkgs only packages mutmut 3.2.0, which hard-guesses `lib/` as the
  # mutate root and cannot scope tests — useless here. 3.7.0 adds
  # config-driven source_paths (incl. single files), test selection, and
  # Python 3.14 support, so it is built from source inside the test env
  # (mutmut runs pytest in-process, so it must share the interpreter that
  # sees the repo's dependencies).
  testPythonEnv = pkgs.python3.withPackages (ps:
    let
      mutmut = ps.buildPythonPackage rec {
        pname = "mutmut";
        version = "3.7.0";
        pyproject = true;
        src = pkgs.fetchFromGitHub {
          owner = "boxed";
          repo = "mutmut";
          tag = version;
          hash = "sha256-jqJWFEYXVA6WizDO34iiyUmElGUBqsqPPyKS8AUJ7ZY=";
        };
        # Upstream pins uv_build<0.10.0; the pinned nixpkgs ships newer.
        postPatch = ''
          substituteInPlace pyproject.toml \
            --replace-fail "uv_build>=0.9.5,<0.10.0" "uv_build>=0.9.5"
        '';
        doCheck = false;
        build-system = [ ps.uv-build ];
        dependencies = with ps; [
          click
          coverage
          libcst
          pytest
          setproctitle
          textual
        ];
      };
    in
    cratedigger.pythonPackages ps
    ++ [
      ps.vulture
      ps.hypothesis
      ps.coverage
      ps.tree-sitter
      ps.tree-sitter-javascript
      ps.pytest
      mutmut
    ]
  );
in
pkgs.mkShell {
  packages = [
    pkgs.postgresql
    pkgs.util-linux
    pkgs.zsh
    pkgs.ruff
    testPythonEnv
    pkgs.sox
    pkgs.ffmpeg
    pkgs.yt-dlp
  ];

  shellHook = ''
    export CRATEDIGGER_BEETS_PYTHON="${testPythonEnv}/bin/python3"
  '';
}
