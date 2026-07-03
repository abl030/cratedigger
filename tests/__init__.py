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

# A real (creatable) dir: beets' config.read() makedirs(BEETSDIR) at import
# in the in-process consumers (lib/beets_distance.py slices), so a
# nonexistent root path would PermissionError. Fixed path (not mkdtemp) so
# repeated suite runs reuse one dir instead of accreting temp dirs.
if "BEETSDIR" not in os.environ:
    _beetsdir = os.path.join(tempfile.gettempdir(), "cratedigger-test-beetsdir")
    os.makedirs(_beetsdir, exist_ok=True)
    os.environ["BEETSDIR"] = _beetsdir
