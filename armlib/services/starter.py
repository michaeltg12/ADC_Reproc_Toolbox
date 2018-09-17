import os

def daemon_control(cmd, daemon_class, *args, **kwargs):
    daemon = daemon_class(*args, **kwargs)
    if 'start' == cmd:
        daemon.start()
    elif 'stop' == cmd:
        daemon.stop()
    elif 'restart' == cmd:
        daemon.restart()
    else:
        print("Unknown command")
        print("usage <daemon_name> start|stop|restart")
        os._exit(2)
