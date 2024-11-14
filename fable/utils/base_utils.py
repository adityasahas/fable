"""
Base utils shared by all other utils
"""
from subprocess import call, check_output, Popen
from .. import config
import os, re, sys
import signal


NULL = open('/dev/null', 'w')

class TimeoutError(Exception):
    pass

class timeout:
    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message
    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)
    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)
    def __exit__(self, type, value, traceback):
        signal.alarm(0)

tmp_path = config.TMP_PATH
port = config.LOCALSERVER_PORT
def localserver(PORT):
    """
    Create tmp dir at $PROJ_HOME, copy domdistiller.js into the repo
    Serve a local server at port if it not occupied by any others
    """
    cur_path = os.path.dirname(__file__)
    call(['mkdir', '-p', tmp_path])
    if not os.path.exists(os.path.join(tmp_path, 'utils', 'domdistiller.js')):
        call(['cp', os.path.join(cur_path, 'domdistiller.js'), tmp_path])
    port_occupied = re.compile(":{}".format(port)).findall(check_output(['netstat', '-nlt']).decode())
    if len(port_occupied) > 0:
        # * Try kill http-server once 
        call(['pkill', 'http-server'])
    port_occupied = re.compile(":{}".format(port)).findall(check_output(['netstat', '-nlt']).decode())
    if len(port_occupied) <= 0:
        Popen(['http-server', '-a', 'localhost', '-p', str(port), tmp_path], stdout=NULL, stderr=NULL)
    else:
        # * Port is not occupied by http-server 
        print(f"Port {port} occupied by other process", file=sys.stderr)

localserver(port)