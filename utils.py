from loguru import logger as log
import keys
from git import Repo
import sys
import os
from datetime import datetime, timedelta
import pytz

Secs = {
    "minute": 60,
    "1min": 60,
    "2min": 120,
    "3min": 180,
    "4min": 240,
    "5min": 300,
    "10min": 600,
    "13min": 780,
    "15min": 900,
    "20min": 1200,
    "30min": 1800,
    "halfhour": 1800,
    "60min": 3600,
    "hour": 3600,
    "2hour": 7200,
    "4hour": 14400,
    "8hour": 28800,
    "12hour": 43200,
    "day": 86400,
    "1day": 86400,
    "3day": 259200,
    "week": 604800,
    "month": 2592000,
    "3month": 7776000,
}

intervals = (
    ("years", 31536000),
    ("months", 2592000),
    # ('weeks', 604800),  # 60 * 60 * 24 * 7
    ("days", 86400),  # 60 * 60 * 24
    ("hours", 3600),  # 60 * 60
    ("minutes", 60),
    ("seconds", 1),
)


def gitupdatecheck():
    log.debug(f'Checking for updates...')
    localdir = keys.localdir
    repo = Repo(localdir)
    origin = repo.remotes.origin
    origin.fetch()
    if repo.head.commit != origin.refs[0].commit:
        log.info(f'New version found. Updating and Restarting...')
        origin.pull()
        Popen(['restarter.py', '--exec', os.path.dirname(sys.argv[0])])
    else:
        log.debug('No updates found')


def estconvert(utc_dt):
    """Summary:
    Args:
        utc_dt (TYPE): Description:
    Returns:
        TYPE: Description:
    """
    ndt = utc_dt.replace(tzinfo=pytz.UTC)
    return ndt.astimezone(pytz.timezone("America/Detroit"))


def datetimeto(dt, fmt, est=False):
    """Convert datetime object
    Args:
        dt (TYPE): Description:
        fmt (TYPE): Description:
        est (bool, [Optional]): Description:
    Returns:
        TYPE: Description:
    """
    if fmt == "epoch":
        return int(dt.timestamp())
    elif fmt == "string":
        return dt.strftime("%a, %b %d, %Y %I:%M %p")


def elapsedTime(start_time, stop_time):
    start_time = datetimeto(start_time, fmt='epoch')
    stop_time = datetimeto(stop_time, fmt='epoch')
    result = []
    if start_time > stop_time:
        seconds = int(start_time) - int(stop_time)
    else:
        seconds = int(stop_time) - int(start_time)
    tseconds = seconds
    if seconds > Secs["minute"] and seconds < Secs["hour"]:
        granularity = 1
    else:
        granularity = 2
    for name, count in intervals:
        value = seconds // count
        if value:
            seconds -= value * count
            if value == 1:
                name = name.rstrip("s")
            result.append("{} {}".format(int(value), name))

    return ", ".join(result[:granularity])
