# Admitted Beets package factory for consumers using Cratedigger's package set.
#
# Returns the pinned nixpkgs' python beets package (python3Packages.beets),
# which carries every built-in plugin's dependency closure — the production
# plugin list (musicbrainz mbsync discogs fetchart embedart lyrics lastgenre
# scrub info missing duplicates edit fromfilename ftintitle the inline) is all
# built-ins, so no pluginOverrides are needed. Because this is the *python
# package* (pkgs.beets is just `toPythonApplication python3Packages.beets`),
# the same store path can serve the Cratedigger Python environment, checker,
# harness, and a deployment-owned plain `beet` command.
#
# The two mirror patches originated in the operator's retired Home Manager
# Beets module and remain available here as opt-in deployment knobs, null/off
# by default so a stranger gets stock plugin behaviour:
#   - discogsMirrorUrl: point the discogs plugin's python3-discogs-client at
#     a Discogs mirror (e.g. https://discogs.ablz.au) instead of
#     api.discogs.com. A mirror built from Discogs' CC0 XML dumps CANNOT
#     serve artwork: the dumps contain no `<images>` elements at all (verified
#     against a live dump, issue #1200), so the plugin's cover_art_url field
#     is never populated for a mirror-backed lookup, and fetchart falls
#     through to a title-guessed source (e.g. itunes), which can put the
#     wrong pressing's art on an album. harness/discogs_patches.py's
#     configure_discogs_cover_art_fallback compensates by wrapping
#     DiscogsPlugin.select_cover_art itself (unconditionally, whether or not
#     this file's mirror patch is applied): a miss from whatever client is
#     configured -- mirror-patched here or stock -- triggers exactly one
#     authenticated lookup against the real api.discogs.com for that release
#     id, fails soft on any error, and never blocks or fails an import. Set
#     this knob at your own risk of the same gap unless your consumer also
#     wires that harness fallback.
#   - lrclibUrl: point the lyrics plugin's LRCLIB base at a local instance
#     (e.g. http://192.168.1.43:3300/api) instead of lrclib.net.
# `--replace-fail` is the drift alarm: if a future `nix flake update` ships
# a beets whose plugin source no longer contains these strings, the build
# fails loudly instead of silently reverting to the public APIs.
{
  pkgs,
  discogsMirrorUrl ? null,
  lrclibUrl ? null,
}:

let
  base = pkgs.python3Packages.beets;

  lrclibPatch = ''
    substituteInPlace beetsplug/lyrics.py \
      --replace-fail 'BASE_URL = "https://lrclib.net/api"' \
                     'BASE_URL = "${lrclibUrl}"'
  '';

  discogsPatch = ''
    substituteInPlace beetsplug/discogs/__init__.py \
      --replace-fail 'self.discogs_client = Client(USER_AGENT, user_token=user_token)' \
                     'self.discogs_client = Client(USER_AGENT, user_token=user_token); self.discogs_client._base_url = "${discogsMirrorUrl}"' \
      --replace-fail 'self.discogs_client = Client(USER_AGENT, c_key, c_secret, token, secret)' \
                     'self.discogs_client = Client(USER_AGENT, c_key, c_secret, token, secret); self.discogs_client._base_url = "${discogsMirrorUrl}"'
  '';

  postPatch =
    (if lrclibUrl != null then lrclibPatch else "")
    + (if discogsMirrorUrl != null then discogsPatch else "");
in
if postPatch == "" then
  base
else
  base.overridePythonAttrs (old: {
    postPatch = (old.postPatch or "") + postPatch;
    # The patched tree is a private runtime variant; upstream's test suite
    # (and its network-touching doctests) already ran for the unpatched
    # base — two URL-literal swaps don't invalidate it, and re-running it
    # roughly doubles eval-to-deploy time on every mirror-knob change.
    doCheck = false;
  })
