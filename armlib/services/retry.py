from functools import partial
import time


def __retry_internal(f, exceptions=(Exception),
                     tries=-1, delay=0, max_delay=None, backoff=1,
                     logger=None):
    """
    Executes a function and retries it if it failed.

    :param f: the function to execute.
    :param exceptions: an exception or a tuple of exceptions to catch. default: Exception.
    :param tries: the maximum number of attempts. default: -1 (infinite).
    :param delay: initial delay between attempts. default: 0.
    :param max_delay: the maximum value of delay. default: None (no limit).
    :param backoff: multiplier applied to delay between attempts. default: 1 (no backoff).
    :param logger: logger.warning(fmt, error, delay) will be called on failed attempts.
                   default: retry.logging_logger. if None, logging is disabled.
    :returns: the result of the f function.
    """
    _tries, _delay = tries, delay
    while _tries:
        try:
            return f()
        except exceptions as e:
            _tries -= 1
            if not _tries:
                if logger is not None:
                    logger.critical('[TIMEOUT ERROR] Failed executing %s %s'%(str(f), str(e)))
                raise

            if logger is not None:
                logger.info('%s, retrying in %s seconds...', str(e), _delay)

            time.sleep(_delay)
            _delay *= backoff

            if max_delay is not None:
                _delay = min(_delay, max_delay)


def retry_call(f, fargs=None, fkwargs=None,
               exceptions=(Exception), tries=-1, delay=0,
               max_delay=None, backoff=1, logger=None):
    """
    Calls a function and re-executes it if it failed.

    :param f: the function to execute.
    :param fargs: the positional arguments of the function to execute.
    :param fkwargs: the named arguments of the function to execute.
    :param exceptions: an exception or a tuple of exceptions to catch. default: Exception.
    :param tries: the maximum number of attempts. default: -1 (infinite).
    :param delay: initial delay between attempts. default: 0.
    :param max_delay: the maximum value of delay. default: None (no limit).
    :param backoff: multiplier applied to delay between attempts. default: 1 (no backoff).
    :param logger: logger.warning(fmt, error, delay) will be called on failed attempts.
                   default: retry.logging_logger. if None, logging is disabled.
    :returns: the result of the f function.
    """
    if fargs is None:
        args = list()
    else:
        args = fargs

    if fkwargs is None:
        kwargs = dict()
    else:
        kwargs = fkwargs
    return __retry_internal(partial(f, *args, **kwargs), exceptions, tries, delay, max_delay, backoff, logger)