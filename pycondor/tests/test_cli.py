
import pytest
import os
from datetime import datetime
from click.testing import CliRunner

import pycondor
from pycondor.utils import clear_pycondor_environment_variables
from pycondor.cli import (line_to_datetime, progress_bar_str, Status, _states,
                          status_generator, monitor, submit)

clear_pycondor_environment_variables()

here = os.path.abspath(os.path.dirname(__file__))
example_script = os.path.join(here, 'example_script.py')
example_dagman_submit = os.path.join(here, 'test_dagman.submit')


def test_line_to_datetime():
    test_line = '''10/22/17 09:38:40  Done     Pre   Queued    Post   Ready
    Un-Ready   Failed'''
    test_datetime = datetime(17, 10, 22, 9, 38, 40)
    dt = line_to_datetime(test_line)
    assert dt == test_datetime


def test_progress_bar_str_type_fail():
    with pytest.raises(TypeError) as excinfo:
        progress_bar_str('not-a-status-object', datetime.now(), datetime.now())
    error = 'status must be of type Status'
    assert error == str(excinfo.value)


def test_progress_bar_str():
    # Test to check the output of progress_bar_str

    # Create status that is 99.5% done. Want to make sure this is displayed
    # as 99% done, not 100% done. See issue #51.
    jobs = [0]*len(_states)
    jobs[0], jobs[2] = 199, 1
    status = Status(*jobs)
    prog_bar_str = progress_bar_str(status, datetime.now(), datetime.now(),
                                    length=30, prog_char='#')

    test_str = '\r[############################# ] 99% Done | 199 done, ' + \
               '1 queued, 0 ready, 0 unready, 0 failed | 0.0m'
    assert prog_bar_str == test_str


def test_progress_bar_str_null_status():
    # Test that a null status (all zeroes) is handled properly
    jobs = [0]*len(_states)
    status = Status(*jobs)
    prog_bar_str = progress_bar_str(status, datetime.now(), datetime.now(),
                                    length=30, prog_char='#')

    test_str = ('\r[                              ] 0% Done | 0 done, '
                '0 queued, 0 ready, 0 unready, 0 failed | 0.0m')
    assert prog_bar_str == test_str


def test_status_generator():
    dag_out_file = os.path.join(here,
                                'exampledagman.submit.dagman.out')

    status_gen = status_generator(dag_out_file)
    status, datetime_current = next(status_gen)

    test_status = Status(Done=2, Pre=0, Queued=1, Post=0, Ready=0,
                         UnReady=0, Failed=0)
    test_datetime = datetime(17, 11, 22, 11, 17, 59)

    assert status == test_status
    assert datetime_current == test_datetime


def test_monitor_pass():
    runner = CliRunner()
    result = runner.invoke(monitor, [example_dagman_submit])
    assert result.exit_code == 0
    expected_output = ('\r[##############################] 100% Done | '
                       '3 done, 0 queued, 0 ready, 0 unready, 0 failed | 1.2m')
    assert result.output == expected_output


def test_monitor_file_raises():
    non_exist_file = 'file.submit'

    runner = CliRunner()
    result = runner.invoke(monitor, [non_exist_file])
    assert result.exit_code == 2
    excerpt = 'Path "{}" does not exist'.format(non_exist_file)
    assert excerpt in result.output.replace('\r', '')


def test_submit_file_raises():
    non_exist_executable = '/this/does/not/exist.py'

    runner = CliRunner()
    result = runner.invoke(submit, [non_exist_executable])
    assert result.exit_code == 2
    excerpt = 'Path "{}" does not exist'.format(non_exist_executable)
    assert excerpt in result.output.replace('\r', '')


def test_submit_equality(tmpdir):
    executable = example_script
    basename = os.path.basename(executable)
    name, _ = os.path.splitext(basename)

    # Create submit file using pycondor.Job
    job = pycondor.Job(name=name,
                       executable=executable,
                       submit=str(tmpdir),
                       log=str(tmpdir),
                       output=str(tmpdir),
                       error=str(tmpdir),
                       request_memory='3GB')
    job.build(fancyname=False)

    with open(job.submit_file, 'r') as f:
        submit_file_1 = f.readlines()

    # Create submit file using pycondor submit
    runner = CliRunner()
    args = ['--submit', str(tmpdir),
            '--output', str(tmpdir),
            '--error', str(tmpdir),
            '--log', str(tmpdir),
            '--request_memory', '3GB',
            # Don't want to actually submit job
            '--dryrun',
            executable,
            ]
    result = runner.invoke(submit, args)
    assert result.exit_code == 0

    submit_file_2 = os.path.join(str(tmpdir),
                                 '{}.sub'.format(name))
    with open(submit_file_2, 'r') as f:
        submit_file_2 = f.readlines()

    # Make sure the two submit files are the same
    assert submit_file_1 == submit_file_2


def test_submit_args(tmpdir):
    executable = example_script
    basename = os.path.basename(executable)
    name, _ = os.path.splitext(basename)

    runner = CliRunner()
    args = ['--submit', str(tmpdir),
            executable,
            '--dryrun',
            '--',
            '--option', 'value',
            ]
    result = runner.invoke(submit, args)
    assert result.exit_code == 0

    submit_file = os.path.join(str(tmpdir),
                               '{}.sub'.format(name))
    with open(submit_file, 'r') as f:
        submit_file_lines = f.read().splitlines()

    assert 'arguments = --option value' in submit_file_lines
