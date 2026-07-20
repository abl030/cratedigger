"""Test-suite environment defaults.

``beets_subprocess_env()`` (tier-2 plan U5, R6) refuses to build a beets
subprocess env without a config dir — production renders [Beets]
config_dir; dev/test provides BEETSDIR. Tests mock the subprocess leaf
(sp.run/sp.Popen) but the env helper runs for real before it, so give the
whole suite a deterministic BEETSDIR default here. Tests that assert the
unset-raises contract clear it explicitly (patch.dict(clear=True)).

setdefault: a caller-provided BEETSDIR (or a runtime config, which takes
precedence anyway) is never overridden.
"""

import os
import tempfile
from pathlib import Path

# A real (creatable) dir with a minimal valid config: Beets and Cratedigger's
# destructive preflight both consume BEETSDIR in-process. Fixed path (not
# mkdtemp) keeps repeated suite runs from accreting temp directories.
if "BEETSDIR" not in os.environ:
    os.environ["BEETSDIR"] = os.path.join(
        tempfile.gettempdir(), "cratedigger-test-beetsdir",
    )
_beetsdir = os.environ["BEETSDIR"]
os.makedirs(_beetsdir, exist_ok=True)
_beets_config = Path(_beetsdir, "config.yaml")
if not _beets_config.exists():
    _beets_config.write_text("plugins: []\n", encoding="utf-8")
