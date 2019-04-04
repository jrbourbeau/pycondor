
from __future__ import division, print_function
import os
import sys
import time
from collections import namedtuple
import click
from datetime import datetime

from .job import Job

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

_states = ['Done', 'Pre', 'Queued', 'Post', 'Ready', 'UnReady', 'Failed']
Status = namedtuple('Status', _states)


def line_to_datetime(line):
    '''Function to extract a datetime from a dagman out file line

    Parameters
    ----------
    line : str
        Any line from a .dagman.out file.

    Returns
    -------
    dt : datetime.datetime
        Datetime stamp from line in out file.
    '''
    date_str = line.split(' ')[0]
    time_str = line.split(' ')[1]
    month, day, year = map(int, date_str.split('/'))
    hour, minute, second = map(int, time_str.split(':'))
    dt = datetime(year, month, day, hour, minute, second)

    return dt


def status_generator(dag_out_file):
    '''Generator to yield dagman status

    Parameters
    ----------
    dag_out_file : str
        Path to dagman out file to parse.

    Returns
    -------
    status : Status
        Status namedtuple that contains the current number of jobs that are
        done, queued, ready, failed, etc. If no line is found indicating the
        current dagman status, an empty Status object is returned.
    datetime_current : datetime.datetime
        Current datetime. If
    '''
    status_str = 'Done     Pre   Queued    Post   Ready   Un-Ready   Failed'
    with open(dag_out_file, 'r') as file_object:
        while True:
            lines = file_object.readlines()
            node_counts = []
            datetime_current = None
            # Reverse order of lines to get most recent status of the dagman
            lines = lines[::-1]
            for idx, line in enumerate(lines):
                if idx == 0:
                    datetime_current = line_to_datetime(line)
                if status_str in line:
                    num_line = lines[idx-2]
                    node_counts = [i for i in num_line.split(' ') if i != ''
                                   and ':' not in i and '/' not in i]
                    node_counts = map(int, node_counts)
                    break

            if node_counts:
                status = Status(*node_counts)
            else:
                status = Status(*[0]*len(_states))
                datetime_current = datetime.now()

            yield status, datetime_current


def progress_bar_str(status, datetime_start, datetime_current, length=30,
                     prog_char='#'):
    '''Function to convert a Status object into a progress bar string

    Parameters
    ----------
    status : Status
        Status namedtuple that contains the current number of jobs that are
        done, queued, ready, failed, etc.
    datetime_start : datetime.datetime
        Datetime for the first line of the dagman out file.
    datetime_current : datetime.datetime
        Datetime for the current line of the dagman out file.
    length : int, optional
        Width of the progress bar (default is 30).
    prog_char : str, optional
        Character used to fill the progress bar (default is '#').

    Returns
    -------
    prog_bar_str : str
        Progress bar string.
    '''
    if not isinstance(status, Status):
        raise TypeError('status must be of type Status')
    n_total = sum(status)
    n_done = status.Done
    try:
        frac_done = n_done / n_total
    except ZeroDivisionError:
        frac_done = 0
    width = int(frac_done * length)

    bar_str = '\r[{0:<{1}}] {2}% Done'.format(prog_char*width, length,
                                              int(100*frac_done))
    count_str = '{} done, {} queued, {} ready, {} unready, {} failed'.format(
            status.Done, status.Queued, status.Ready,
            status.UnReady, status.Failed)

    dt = datetime_current - datetime_start
    time_str = '{:0.1f}m'.format(dt.seconds / 60)

    prog_bar_str = ' | '.join([bar_str, count_str, time_str])

    return prog_bar_str


@click.group(
    context_settings=CONTEXT_SETTINGS,
)
def cli():
    '''PyCondor command line tool'''
    pass


@cli.command(
    context_settings=CONTEXT_SETTINGS,
    short_help='Monitor Dagman progress',
)
# Using time_ variable name so no confusion with time module
@click.option(
    '-t',
    '--time',
    'time_',
    default=30,
    type=float,
    show_default=True,
    help='Time (in seconds) in between log checks',
)
@click.option(
    '-l',
    '--length',
    default=30,
    type=int,
    show_default=True,
    help='Length of the progress bar',
)
@click.option(
    '--prog_char',
    default='#',
    show_default=True,
    help='Progress bar character',
)
@click.argument(
    'file',
    type=click.Path(exists=True),
)
def monitor(time_, length, prog_char, file):
    '''Prints Dagman progress bar to stdout
    '''
    dag_out_file = file + '.dagman.out'
    # Make sure dagman out file exists
    # It isn't created until the dagman beings running
    while not os.path.exists(dag_out_file):
        sys.stdout.write(
            '\rWaiting for dagman {} to begin running...'.format(file))
        sys.stdout.flush()
        time.sleep(time_)

    with open(dag_out_file, 'r') as f:
        datetime_start = line_to_datetime(f.readline())
    current_status = Status(*[0]*len(_states))
    try:
        for status, datetime_current in status_generator(dag_out_file):
            # If no line with dagman status is found, wait and try again
            if sum(status) != 0:
                current_status = status

            prog_str = progress_bar_str(current_status,
                                        datetime_start=datetime_start,
                                        datetime_current=datetime_current,
                                        length=length,
                                        prog_char=prog_char)
            sys.stdout.write(prog_str)
            sys.stdout.flush()
            # Exit if at least one job is Done or Failed (something ran) and
            # all jobs either Done, Failed, or UnReady (nothing left to run)
            n_jobs = sum(current_status)
            n_ran = current_status.Done + current_status.Failed
            n_finished = n_ran + current_status.UnReady
            if n_jobs and n_ran and n_finished == n_jobs:
                sys.exit(0)
            else:
                time.sleep(time_)
    except KeyboardInterrupt:
        print('\nExiting pycondor monitor...')
        sys.exit()


@cli.command(
    context_settings=CONTEXT_SETTINGS,
    short_help='Submit a Job',
)
@click.option(
    '--submit',
    default=None,
    type=click.Path(),
    show_default=True,
    help='Directory to store submit files',
)
@click.option(
    '--log',
    default=None,
    type=click.Path(),
    show_default=True,
    help='Directory to store log files',
)
@click.option(
    '--output',
    default=None,
    type=click.Path(),
    show_default=True,
    help='Directory to store output files',
)
@click.option(
    '--error',
    default=None,
    type=click.Path(),
    show_default=True,
    help='Directory to store error files',
)
@click.option(
    '--request_memory',
    default=None,
    show_default=True,
    help='Memory request to be included in submit file',
)
@click.option(
    '--request_disk',
    default=None,
    show_default=True,
    help='Disk request to be included in submit file',
)
@click.option(
    '--request_cpus',
    default=None,
    show_default=True,
    help='Number of CPUs to request in submit file',
)
@click.option(
    '--universe',
    default=None,
    show_default=True,
    help='Universe execution environment to be specified in submit file',
)
@click.option(
    '--getenv',
    default=None,
    type=bool,
    show_default=True,
    help='Set getenv to True or False',
)
@click.option(
    '--dryrun',
    is_flag=True,
    show_default=True,
    help='Only build submit file, but do not submit it for execution'
)
@click.argument(
    'executable',
    required=True,
    nargs=1,
    type=click.Path(exists=True),
)
@click.argument(
    'args',
    nargs=-1,
)
def submit(submit, log, output, error, request_memory, request_disk,
           request_cpus, universe, getenv, dryrun, executable, args):
    '''Quickly submit a Job to HTCondor from the command line
    '''
    basename = os.path.basename(executable)
    name, _ = os.path.splitext(basename)

    job = Job(name=name,
              executable=executable,
              submit=submit,
              log=log,
              output=output,
              error=error,
              request_memory=request_memory,
              request_disk=request_disk,
              request_cpus=request_cpus,
              universe=universe,
              getenv=getenv,
              )
    if args:
        arguments = str(' '.join(args))
        job.add_arg(arguments)

    if dryrun:
        job.build(fancyname=False)
    else:
        job.build_submit()
