"""Search query builder for Soulseek.

Builds search queries from artist + album title, applying transforms
to work around Soulseek's server-side search filtering.

Key insight: Soulseek bans certain artist names server-side (Beatles,
AFI, Kanye, etc.). Searches containing banned terms return 0 results.
Replacing the first character with * bypasses the filter:
  "Beatles" → "*eatles" (17786 results vs 0).

We wildcard ALL artist tokens unconditionally — there's no downside
(*ountain matches Mountain) and it avoids needing to maintain a
banned word list.

Pure functions — no I/O, no external dependencies.
"""

import re

# Soulseek's distributed search times out with too many tokens.
# 4 is the safe maximum.
MAX_SEARCH_TOKENS = 4


def strip_special_chars(text):
    """Remove punctuation that poisons Soulseek searches.

    Apostrophes, brackets, and other special characters cause
    0 results or wrong matches.
    """
    clean = re.sub(r"['\"\(\)\[\]\{\}!@#$%^&*_]", " ", text)
    return " ".join(clean.split())


def strip_short_tokens(tokens):
    """Remove tokens with <= 2 characters.

    Soulseek silently drops these server-side, so they waste a
    token slot without contributing to the search.
    e.g. "A Tribe Called Quest" → "Tribe Called Quest"
    """
    long = [t for t in tokens if len(t) > 2]
    return long if long else tokens  # keep originals if ALL are short


def wildcard_artist_tokens(artist_tokens):
    """Replace the first character of each artist token with *.

    Bypasses Soulseek's server-side artist name bans.
    e.g. ["Mountain", "Goats"] → ["*ountain", "*oats"]

    Tokens that are already too short to wildcard (<=1 char) are dropped.
    """
    result = []
    for t in artist_tokens:
        if len(t) > 1:
            result.append("*" + t[1:])
        # Drop single-char tokens — they'd become just "*" which matches everything
    return result


def cap_tokens(tokens, max_tokens=MAX_SEARCH_TOKENS):
    """Keep the most distinctive tokens, cap at max count.

    Drops the shortest (most common/ambiguous) tokens first,
    preserving original word order.
    """
    if len(tokens) <= max_tokens:
        return tokens

    # Sort by length descending, keep the longest
    kept = sorted(tokens, key=len, reverse=True)[:max_tokens]

    # Restore original order, handling duplicates
    seen = {}
    ordered = []
    for t in tokens:
        count = seen.get(t, 0)
        if count < kept.count(t):
            ordered.append(t)
            seen[t] = count + 1
        if len(ordered) >= max_tokens:
            break

    return ordered


def build_query(artist, title, prepend_artist=True, max_tokens=MAX_SEARCH_TOKENS):
    """Build a Soulseek search query from artist + album title.

    Returns the final query string.

    Pipeline:
      1. Clean punctuation from both artist and title
      2. Tokenize separately
      3. Strip short tokens (<=2 chars)
      4. Wildcard artist tokens (bypass bans)
      5. Combine and cap total token count

    Artist tokens are always prepended and wildcarded.
    """
    # Clean punctuation
    clean_artist = strip_special_chars(artist)
    clean_title = strip_special_chars(title)

    # Tokenize
    artist_tokens = clean_artist.split()
    title_tokens = clean_title.split()

    # Strip short tokens from each
    artist_tokens = strip_short_tokens(artist_tokens)
    title_tokens = strip_short_tokens(title_tokens)

    # Drop title tokens that duplicate artist tokens (case-insensitive).
    # e.g. "The Castiles - The Castiles Live" → artist has "Castiles",
    # title has "Castiles" + "Live" → drop duplicate, keep "Live".
    # This avoids wasting token slots and leaking un-wildcarded artist names.
    artist_lower = {t.lower() for t in artist_tokens}
    title_tokens = [t for t in title_tokens if t.lower() not in artist_lower]

    # Wildcard artist tokens
    artist_tokens = wildcard_artist_tokens(artist_tokens)

    if prepend_artist and artist_tokens:
        all_tokens = artist_tokens + title_tokens
    else:
        all_tokens = title_tokens

    if not all_tokens:
        return None

    # Cap total tokens
    all_tokens = cap_tokens(all_tokens, max_tokens)

    return " ".join(all_tokens)
