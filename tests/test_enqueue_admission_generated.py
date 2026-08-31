"""Generated candidate-size admission contract for issue #1301."""

from __future__ import annotations

import unittest
from collections.abc import Sequence
from unittest.mock import patch

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from cratedigger import SlskdFile
from lib.enqueue import EnqueueAttempt, try_enqueue
from lib.grab_list import DownloadFile
from lib.matching import MatchResult
from lib.slskd_transfers import SlskdEnqueueOutcome
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.test_enqueue_fanout import (
    _const_match,
    _ctx_with_download_ownership,
    _make_cfg,
    _make_tracks,
)


def _run_size_world(
    sizes: list[int | None],
) -> tuple[EnqueueAttempt, FakePipelineDB, int]:
    cfg = _make_cfg()
    db = FakePipelineDB()
    db.seed_request(make_request_row(id=1, status="wanted"))
    ctx = _ctx_with_download_ownership(cfg=cfg, db=db)
    username = "peer"
    file_dir = "Music\\peer\\Album"
    ctx.user_upload_speed[username] = 10_000
    files: list[SlskdFile] = []
    for ordinal, size in enumerate(sizes, start=1):
        file: SlskdFile = {
            "filename": f"{ordinal:02d} - Track {ordinal}.mp3",
            "bitRate": 216,
        }
        if size is not None:
            file["size"] = size
        files.append(file)
    match = MatchResult(
        matched=True,
        directory={"directory": file_dir, "files": files},
        file_dir=file_dir,
        candidates=[],
    )
    enqueue_calls = 0

    def accept_enqueue(
        *,
        username: str,
        files: Sequence[SlskdFile],
        file_dir: str,
        **_kwargs: object,
    ) -> SlskdEnqueueOutcome:
        nonlocal enqueue_calls
        enqueue_calls += 1
        downloads: list[DownloadFile] = []
        for ordinal, file in enumerate(files, start=1):
            size = file.get("size")
            assert isinstance(size, int) and not isinstance(size, bool)
            downloads.append(DownloadFile(
                filename=file["filename"],
                id=f"transfer-{ordinal}",
                file_dir=file_dir,
                username=username,
                size=size,
            ))
        return SlskdEnqueueOutcome(
            status="accepted",
            downloads=downloads,
        )

    with patch("lib.enqueue._fanout_browse_users", return_value=set()), \
         patch(
             "lib.enqueue.slskd_enqueue_with_outcome",
             side_effect=accept_enqueue,
         ):
        attempt = try_enqueue(
            _make_tracks(),
            {username: {"mp3": [file_dir]}},
            "mp3",
            ctx,
            match_fn=_const_match(match),
        )
    return attempt, db, enqueue_calls


class TestAdvertisedSizeAdmissionProperty(unittest.TestCase):
    """A selected audio manifest is admitted iff every size is positive."""

    @given(
        sizes=st.lists(
            st.one_of(st.none(), st.integers(min_value=-1, max_value=2)),
            min_size=1,
            max_size=6,
        ),
    )
    @example(sizes=[1, 0])
    @example(sizes=[1, None])
    def test_non_positive_or_missing_size_never_reaches_slskd(
        self, *, sizes: list[int | None],
    ) -> None:
        attempt, db, enqueue_calls = _run_size_world(sizes)
        should_admit = all(size is not None and size > 0 for size in sizes)

        self.assertEqual(attempt.matched, should_admit)
        self.assertEqual(enqueue_calls, int(should_admit))
        self.assertEqual(
            db.request(1)["status"],
            "downloading" if should_admit else "wanted",
        )
        if not should_admit:
            self.assertEqual(db.record_transfer_enqueue_calls, [])
