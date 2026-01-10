"""
Live integration test for the NE-300 / Aurora NE-series TCP/IP interface.

This test talks to the instrument. It is **skipped by default**.
Enable it explicitly:

  NEPH_LIVE=1 pytest -q -k neph_live

You can override defaults:
  NEPH_CONFIG=/path/to/mkndaq.yml
  NEPH_NAME=ne300
  NEPH_INITIALIZE=0|1   (default: 0)
  NEPH_LOOKBACK_MIN=60  (default: 60)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

try:
    from mkndaq.inst.neph import NEPH  # type: ignore
except ImportError:  # pragma: no cover
    from mkndaq.mkndaq.inst.neph import NEPH  # type: ignore


@pytest.mark.live
def test_neph_live_get_logged_record() -> None:
    if os.getenv("NEPH_LIVE", "0").lower() not in {"1", "true", "yes"}:
        pytest.skip("Live test skipped. Set NEPH_LIVE=1 to enable.")

    cfg_path = Path(os.getenv("NEPH_CONFIG", "mkndaq-fallback.yml")).expanduser()
    if not cfg_path.exists():
        pytest.skip(f"Config file not found: {cfg_path}")

    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    name = os.getenv("NEPH_NAME", "ne300")

    initialize = os.getenv("NEPH_INITIALIZE", "0").lower() in {"1", "true", "yes"}
    neph = NEPH(name="ne300", config=cfg, verbosity=0) #, initialize=initialize)

    lookback_min = int(os.getenv("NEPH_LOOKBACK_MIN", "5"))
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=lookback_min)

    records = neph.get_logged_data(start=start, end=end, verbosity=0, raw=False)

    assert isinstance(records, list)
    assert records, "No logged records returned (check instrument logging, lookback window, and connection)."

    rec = records[-1]
    assert isinstance(rec, dict)
    assert "dtm" in rec, "Expected 'dtm' in logged record."
    assert 4035 in rec, "Expected operation code 4035 in logged record."
    assert 2002 in rec, "Expected logging period 2002 in logged record."

    extra_pids = [k for k in rec.keys() if isinstance(k, int) and k not in (4035, 2002)]
    assert extra_pids, "Logged record contains no additional parameter IDs (unexpected)."
