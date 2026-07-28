"""Custom timetables replicating AutoSys calendar behavior.

- HolidaySkipCronTimetable: cron schedule that skips dates listed in
  include/data/holidays.csv (AutoSys run_calendar / exclude_calendar).
- BlackoutCronTimetable: cron schedule that skips runs falling inside blackout
  date ranges (AutoSys exclude_calendar / run_window).

Both are registered as Airflow plugins in plugins/validation_timetables_plugin.py
so they survive DAG serialization.
"""

from __future__ import annotations

import csv
from datetime import date
from functools import lru_cache
from pathlib import Path

from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction
from airflow.timetables.interval import CronDataIntervalTimetable

HOLIDAY_FILE = Path("/usr/local/airflow/include/data/holidays.csv")

# Guard against schedules that would loop forever if every date is excluded.
_MAX_SKIPS = 366


@lru_cache(maxsize=1)
def load_holidays() -> frozenset[date]:
    if not HOLIDAY_FILE.exists():
        return frozenset()
    dates = set()
    with HOLIDAY_FILE.open() as f:
        for row in csv.reader(f):
            if not row or row[0].startswith("#"):
                continue
            dates.add(date.fromisoformat(row[0].strip()))
    return frozenset(dates)


class HolidaySkipCronTimetable(CronDataIntervalTimetable):
    """Cron schedule that skips runs whose run date is a listed holiday."""

    @property
    def summary(self) -> str:
        return f"{super().summary} (skips holidays)"

    def _is_excluded(self, run_date: date) -> bool:
        return run_date in load_holidays()

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        info = super().next_dagrun_info(
            last_automated_data_interval=last_automated_data_interval,
            restriction=restriction,
        )
        skips = 0
        while info is not None and self._is_excluded(info.run_after.date()):
            skips += 1
            if skips > _MAX_SKIPS:
                return None
            info = super().next_dagrun_info(
                last_automated_data_interval=info.data_interval,
                restriction=restriction,
            )
        return info


class BlackoutCronTimetable(HolidaySkipCronTimetable):
    """Cron schedule that also skips runs inside blackout date ranges.

    Blackout ranges are defined inline for simplicity (see BLACKOUT_RANGES).
    In production these would live in a config file or Variable, same pattern
    as the holiday CSV.
    """

    # (start, end) inclusive date ranges during which no runs are created.
    BLACKOUT_RANGES: tuple[tuple[date, date], ...] = (
        (date(2026, 12, 24), date(2026, 12, 31)),  # year-end change freeze
    )

    @property
    def summary(self) -> str:
        return f"{CronDataIntervalTimetable.summary.fget(self)} (skips holidays + blackouts)"

    def _is_excluded(self, run_date: date) -> bool:
        if super()._is_excluded(run_date):
            return True
        return any(start <= run_date <= end for start, end in self.BLACKOUT_RANGES)
