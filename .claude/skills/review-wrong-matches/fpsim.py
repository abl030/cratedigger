"""Compare chromaprint raw fingerprints: is this the same recording?

Usage: python3 -I fpsim.py A.fp:B.fp [C.fp:D.fp ...]

Each .fp file holds the comma-separated FINGERPRINT= value from
`fpcalc -raw -length 600 FILE`. Prints the best bit-agreement over a
+/-40 frame (~5s) alignment window. A shorter file compared against a
longer one scores as a prefix/substring match.

Reading the score: ~0.95+ is the same recording (lossy vs lossless included),
~0.5 is unrelated audio. Anything between deserves a listen.
"""

import sys


def load(path: str) -> list[int]:
    with open(path, encoding="ascii") as handle:
        raw = handle.read().strip()
    return [int(x) for x in raw.split(",") if x]


def similarity(a: list[int], b: list[int], window: int = 40) -> float:
    best = 0.0
    for offset in range(-window, window + 1):
        frames = 0
        agree = 0
        for i, value in enumerate(a):
            j = i + offset
            if 0 <= j < len(b):
                frames += 1
                agree += 32 - ((value ^ b[j]) & 0xFFFFFFFF).bit_count()
        if frames > 50:
            best = max(best, agree / (frames * 32))
    return round(best, 3)


def main(pairs: list[str]) -> None:
    for pair in pairs:
        left, right = pair.split(":", 1)
        print(f"{left}\t{right}\t{similarity(load(left), load(right))}")


if __name__ == "__main__":
    main(sys.argv[1:])
