from loguru import logger as log

log.remove()

shortlogformat = "<level>{time:YYYY-MM-DD HH:mm:ss.SSS}</level><fg 248>|</fg 248><level>{level: <7}</level><fg 248>|</fg 248> <level>{message}</level>"
longlogformat = "<level>{time:YYYY-MM-DD HH:mm:ss.SSS}</level><fg 248>|</fg 248> <level>{level: <7}</level> <fg 248>|</fg 248> <level>{message: <72}</level> <fg 243>|</fg 243> <fg 109>{name}:{function}:{line}</fg 109>"


def checkerrorlog(record):
    if record["level"] == "WARNING" or record["level"] == "ERROR" or record["level"] == "CRITICAL":
        return True
    else:
        return False


# Error Logging error.log
log.add(
    sink='/var/log/investomatic/error.log',
    level=40,
    buffering=1,
    enqueue=True,
    backtrace=True,
    diagnose=True,
    colorize=False,
    format=longlogformat,
    delay=False,
    filter=checkerrorlog,
)
