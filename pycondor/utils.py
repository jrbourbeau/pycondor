
import os
import re
import sys
import subprocess
import logging
import shutil
import shlex
import platform
from functools import wraps
# Note that spawn isn't in namespace if import distutils is used
# Must use from distutils import spawn
from distutils import spawn


# Specify logging settings
formatter = logging.Formatter('%(levelname)s: pycondor - %(name)s : %(message)s')
logging_level_dict = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}


def _setup_logger(cls, verbose=0):
    """Configures and returns logger instance.

    This function takes a class instance (which must have a `name` attribute)
    and a verbosity level as parameters and returns a connfigured logger
    instance with the appropriate level set.

    Parameters
    ----------
    cls : object
        Class instance with attribute `name` (e.g. Dagman, Job)
    verbose: int
        Verbosity level. Values can be 0, 1, or 2, with 0 being the least
        verbose and 2 being the most verbose.

    Returns
    -------
    logger : logging.Logger
        Configured logger

    Allows classes to each have their own seperate loggers in order to
    allow for varying levels of verbosity. For example, you might want low
    verbosity for a pycondor Job, but high verbosity for a Dagman
    class. setup_logger() helps streamline this process.
    """
    if not hasattr(cls, 'name'):
        raise AttributeError('Input must have a "name" attribute.')
    # Set up logger
    if verbose not in logging_level_dict:
        raise KeyError('Verbose option {} for {} not valid. '
                       'Valid options are {}.'.format(
                           verbose, cls.name, logging_level_dict.keys()))
    logger = logging.getLogger(cls.name)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.setLevel(logging_level_dict[verbose])

    return logger


def checkdir(path, makedirs):
    assert path is not None, 'path must be non-NoneType'
    outdir = os.path.dirname(path)
    if outdir == '':
        # Current working directory exists
        return
    if not os.path.isdir(outdir):
        if makedirs:
            print('The directory {} doesn\'t exist, '.format(outdir)
                  + 'creating it...')
            os.makedirs(outdir)
        else:
            raise IOError('The directory {} doesn\'t exist'.format(outdir))
    return


def get_queue(submitter=None):

    queue_command = 'condor_q'
    if submitter:
        queue_command += ' -submitter {}'.format(submitter)
    proc = subprocess.Popen([queue_command], stdout=subprocess.PIPE,
                            shell=True)
    (out, err) = proc.communicate()

    return out


def string_rep(obj, quotes=False):
    '''Converts basic python objects to a string representation

    '''
    assert obj is not None, 'Input must not be None'

    quote = '"' if quotes else ''

    if isinstance(obj, (tuple, list)):
        obj_str = ', '.join([string_rep(item) for item in obj])
    else:
        obj_str = str(obj)

    return quote + obj_str + quote


def clear_pycondor_environment_variables():
    # Unset any pycondor directory environment variables
    for i in ['submit', 'output', 'error', 'log']:
        env_var = 'PYCONDOR_{}_DIR'.format(i.upper())
        if os.getenv(env_var):
            del os.environ[env_var]


def assert_command_exists(cmd):
    version_major = sys.version_info.major
    version_minor = sys.version_info.minor
    if (version_major, version_minor) >= (3, 3):
        cmd_path = shutil.which(cmd)
    else:
        cmd_path = spawn.find_executable(cmd)

    if cmd_path is None:
        raise OSError(
            'The command \'{}\' was not found on this machine.'.format(cmd))


def requires_command(*commands):
    """Decorator to wrap functions that require a specific set of commands
    """
    def real_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for command in commands:
                assert_command_exists(command)
            return func(*args, **kwargs)
        return wrapper
    return real_decorator


def decode_string(s):
    """Decode bytes array
    """
    try:
        s = s.decode('utf-8')
    except AttributeError:
        pass

    return s


def parse_condor_version(info):
    """ Extract condor version number tuple from ``condor_version`` output string

    Parameters
    ----------
    info : str
        Output from ``condor_version`` command or ``htcondor.version()``

    Returns
    -------
    condor_version : tuple
        Version number tuple (e.g. ``(8, 7, 4)``)
    """
    info = decode_string(info)
    condor_version_str = re.search(r'CondorVersion: \s*([\d.]+)', info).group(1)
    condor_version = tuple(map(int, condor_version_str.split('.')))

    return condor_version


def get_condor_version():
    """Should only be called on a submit machine
    """
    info = None
    try:
        import htcondor
        info = htcondor.version()
    except ImportError:
        assert_command_exists('condor_version')
        proc = subprocess.Popen(['condor_version'], stdout=subprocess.PIPE,
                                shell=True)
        out, err = proc.communicate()
        info = out
    finally:
        if info is None:
            raise OSError('Could not find HTCondor version.')

    condor_version = parse_condor_version(info)

    return condor_version


def split_command_string(string):
    """Uses shlex.split() to split a string into a list according
    to the operating system
    """
    is_posix = platform.system() != 'Windows'
    return shlex.split(string, posix=is_posix)
