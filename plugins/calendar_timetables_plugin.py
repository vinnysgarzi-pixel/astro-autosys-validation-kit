"""Registers the custom calendar timetables so Airflow can (de)serialize them."""

from airflow.plugins_manager import AirflowPlugin

from include.timetables.calendar_timetables import (
    BlackoutCronTimetable,
    HolidaySkipCronTimetable,
)


class CalendarTimetablesPlugin(AirflowPlugin):
    name = "validation_timetables"
    timetables = [HolidaySkipCronTimetable, BlackoutCronTimetable]
