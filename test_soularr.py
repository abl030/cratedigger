"""Tests for verify_filetype() — VBR V0/V2 support (TDD)."""

import sys
import types

# soularr.py uses top-level globals and imports that make it hard to import
# directly. We extract verify_filetype() by compiling just the function.
import importlib.util
import ast
import textwrap

def _extract_verify_filetype():
    """Extract verify_filetype from soularr.py without running module-level code."""
    with open("soularr.py") as f:
        source = f.read()

    tree = ast.parse(source)
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "verify_filetype":
            func_source = ast.get_source_segment(source, node)
            break
    else:
        raise RuntimeError("verify_filetype not found in soularr.py")

    # Create a minimal module with a logger stub
    import logging
    ns = {"logger": logging.getLogger("test"), "logging": logging}
    exec(compile(func_source, "soularr.py", "exec"), ns)
    return ns["verify_filetype"]

verify_filetype = _extract_verify_filetype()


# === Existing behaviour (must not break) ===

def test_mp3_cbr_320_exact_match():
    file = {"filename": "track.mp3", "bitRate": 320}
    assert verify_filetype(file, "mp3 320") == True

def test_mp3_cbr_256_exact_match():
    file = {"filename": "track.mp3", "bitRate": 256}
    assert verify_filetype(file, "mp3 256") == True

def test_mp3_cbr_320_no_match_192():
    file = {"filename": "track.mp3", "bitRate": 192}
    assert verify_filetype(file, "mp3 320") == False

def test_bare_mp3_matches_any():
    file = {"filename": "track.mp3", "bitRate": 128}
    assert verify_filetype(file, "mp3") == True

def test_flac_bitdepth_samplerate():
    file = {"filename": "track.flac", "bitDepth": 24, "sampleRate": 96000}
    assert verify_filetype(file, "flac 24/96") == True

def test_flac_bare_matches_any():
    file = {"filename": "track.flac", "bitRate": 800}
    assert verify_filetype(file, "flac") == True

def test_extension_mismatch():
    file = {"filename": "track.flac", "bitRate": 320}
    assert verify_filetype(file, "mp3 320") == False


# === NEW: VBR V0 tests ===

def test_mp3_v0_matches_vbr_245():
    file = {"filename": "track.mp3", "bitRate": 245}
    assert verify_filetype(file, "mp3 v0") == True

def test_mp3_v0_matches_vbr_230():
    file = {"filename": "track.mp3", "bitRate": 230}
    assert verify_filetype(file, "mp3 v0") == True

def test_mp3_v0_matches_vbr_260():
    file = {"filename": "track.mp3", "bitRate": 260}
    assert verify_filetype(file, "mp3 v0") == True

def test_mp3_v0_rejects_low_bitrate():
    file = {"filename": "track.mp3", "bitRate": 170}
    assert verify_filetype(file, "mp3 v0") == False

def test_mp3_v0_rejects_cbr_320():
    file = {"filename": "track.mp3", "bitRate": 320}
    assert verify_filetype(file, "mp3 v0") == False

def test_mp3_v0_rejects_cbr_192():
    file = {"filename": "track.mp3", "bitRate": 192}
    assert verify_filetype(file, "mp3 v0") == False


# === NEW: VBR V2 tests ===

def test_mp3_v2_matches_vbr_190():
    file = {"filename": "track.mp3", "bitRate": 190}
    assert verify_filetype(file, "mp3 v2") == True

def test_mp3_v2_matches_vbr_170():
    file = {"filename": "track.mp3", "bitRate": 170}
    assert verify_filetype(file, "mp3 v2") == True

def test_mp3_v2_rejects_low_bitrate():
    file = {"filename": "track.mp3", "bitRate": 120}
    assert verify_filetype(file, "mp3 v2") == False


# === VBR flag tests (isVariableBitRate from slskd) ===

def test_mp3_v0_with_vbr_flag():
    file = {"filename": "track.mp3", "bitRate": 245, "isVariableBitRate": True}
    assert verify_filetype(file, "mp3 v0") == True

def test_mp3_v0_cbr_with_vbr_flag_false():
    # A file reporting 245 CBR should NOT match v0
    file = {"filename": "track.mp3", "bitRate": 245, "isVariableBitRate": False}
    assert verify_filetype(file, "mp3 v0") == False


# === Edge: V0 range boundary ===

def test_mp3_v0_lower_boundary():
    file = {"filename": "track.mp3", "bitRate": 220}
    assert verify_filetype(file, "mp3 v0") == True

def test_mp3_v0_upper_boundary():
    file = {"filename": "track.mp3", "bitRate": 280}
    assert verify_filetype(file, "mp3 v0") == True

def test_mp3_v0_below_lower_boundary():
    file = {"filename": "track.mp3", "bitRate": 219}
    assert verify_filetype(file, "mp3 v0") == False

def test_mp3_v0_above_upper_boundary():
    file = {"filename": "track.mp3", "bitRate": 281}
    assert verify_filetype(file, "mp3 v0") == False


# === Edge: CBR values in V0 range should be excluded ===

def test_mp3_v0_rejects_cbr_256():
    # 256 is in V0 range (220-280) but is a standard CBR value
    file = {"filename": "track.mp3", "bitRate": 256}
    assert verify_filetype(file, "mp3 v0") == False

def test_mp3_v0_rejects_cbr_224():
    # 224 is in V0 range (220-280) but is a standard CBR value
    file = {"filename": "track.mp3", "bitRate": 224}
    assert verify_filetype(file, "mp3 v0") == False
