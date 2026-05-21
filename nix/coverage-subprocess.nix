# A Python package that ships a single .pth file. Python's site.py
# auto-executes .pth files on interpreter startup; ours conditionally
# attaches coverage.py to the subprocess based on COVERAGE_PROCESS_START.
#
# Why we need this: cratedigger spawns subprocesses (import_one.py via the
# beets harness, plus the beets `beet` binary which is third-party — we
# skip that). The top-level `coverage run` only traces the parent's PID.
# `coverage.process_startup()` is the documented hook for attaching to
# every Python subprocess that inherits the env var.
#
# Always installing this is safe — if COVERAGE_PROCESS_START is unset,
# the .pth file does a single os.environ lookup and returns. No
# measurable startup cost.

{ pkgs }:

pkgs.python3Packages.buildPythonPackage {
  pname = "cratedigger-coverage-subprocess";
  version = "1.0";
  format = "other";

  # Python's site.py only exec()s .pth lines starting with `import ` —
  # everything else is treated as a path entry. The exec() trampoline is the
  # canonical way to run conditional code from a .pth file. The whole if/then
  # has to fit on one line (no real newlines after the leading `import`).
  src = pkgs.writeTextDir "cratedigger_coverage_subprocess.pth" ''
    import os; exec("if os.environ.get('COVERAGE_PROCESS_START'):\n    import coverage; coverage.process_startup()")
  '';

  # The .pth file must land directly in site-packages. We don't ship any
  # importable module — site.py reads .pth files at the top level only.
  installPhase = ''
    runHook preInstall
    sitePackages="$out/${pkgs.python3.sitePackages}"
    mkdir -p "$sitePackages"
    cp $src/cratedigger_coverage_subprocess.pth "$sitePackages/"
    runHook postInstall
  '';

  propagatedBuildInputs = [ pkgs.python3Packages.coverage ];

  doCheck = false;

  meta = {
    description = "Pth-file shim that attaches coverage.py to Python subprocesses when COVERAGE_PROCESS_START is set";
  };
}
