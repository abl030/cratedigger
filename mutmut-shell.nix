# EXPERIMENT SHELL — mutmut evaluation (not suite machinery).
# Same flake-locked nixpkgs as nix/shell.nix, plus mutmut + pytest so the
# mutation runner imports the repo's real dependency set.
{ pkgs ? import (
    let
      lock = builtins.fromJSON (builtins.readFile ./flake.lock);
      node = lock.nodes.${lock.nodes.${lock.root}.inputs.nixpkgs}.locked;
    in
    builtins.fetchTarball {
      url = "https://github.com/${node.owner}/${node.repo}/archive/${node.rev}.tar.gz";
      sha256 = node.narHash;
    }
  ) {} }:

let
  beetsPackage = import ./nix/beets.nix { inherit pkgs; };
  cratedigger = import ./nix/package.nix { inherit pkgs beetsPackage; };

  # The pinned nixpkgs only ships mutmut as a top-level application
  # (pkgs/by-name/mu/mutmut), whose interpreter would not see the repo's
  # deps. Rebuild it as a library inside the test env instead — same
  # source, hash, and dependency list as the nixpkgs recipe.
  testPythonEnv = pkgs.python3.withPackages (ps:
    let
      # 3.7.0 (latest): config-driven paths_to_mutate incl. single files,
      # tests_dir/pytest CLI-args config, Python 3.14 support, LibCST.
      # nixpkgs only packages 3.2.0, which hard-guesses `lib/` as the
      # mutate root and cannot scope tests — useless for this repo.
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
        # Upstream pins uv_build<0.10.0; the pinned nixpkgs ships 0.11.28.
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
