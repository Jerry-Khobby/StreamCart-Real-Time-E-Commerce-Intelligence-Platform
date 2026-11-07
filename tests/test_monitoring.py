"""Volume anomaly rules used by the observability monitor."""

import os
import sys
from pathlib import Path

sys.path.insert(0, os.getenv("DAG_FOLDER", str(Path(__file__).resolve().parent.parent / "batch" / "dags")))
if not Path(sys.path[0]).exists():
    sys.path[0] = str(Path(__file__).resolve().parent.parent / "dags")

from common import assess_volume  # noqa: E402


def test_a_normal_day_is_not_flagged():
    baseline, ratio, flagged = assess_volume(980, [1000, 1050, 990, 1020, 970, 1010, 1000])
    assert baseline == 1000 and ratio == 0.98 and not flagged


def test_a_day_under_half_the_usual_volume_is_flagged():
    assert assess_volume(400, [1000, 1050, 990, 1020, 970, 1010, 1000])[2]


def test_one_unusual_day_does_not_move_the_baseline():
    baseline, _, flagged = assess_volume(950, [1000, 1010, 990, 1005, 995, 20000, 1000])
    assert baseline == 1000 and not flagged


def test_a_spike_is_not_flagged():
    assert not assess_volume(5000, [1000] * 7)[2]


def test_too_little_history_is_never_flagged():
    assert assess_volume(0, [1000, 1000]) == (None, None, False)


def test_an_empty_day_after_normal_days_is_flagged():
    assert assess_volume(0, [1000] * 7)[2]
