
import filecmp
import os
import re
from collections import Counter

import pytest

from pycondor import Job, Dagman
from pycondor.dagman import _iter_job_args, _get_subdag_string
from pycondor.utils import clear_pycondor_environment_variables

clear_pycondor_environment_variables()

here = os.path.abspath(os.path.dirname(__file__))
example_script = os.path.join(here, 'example_script.py')


@pytest.fixture()
def dagman(tmpdir_factory):
    submit_dir = str(tmpdir_factory.mktemp('submit'))
    dagman = Dagman('exampledagman', submit=submit_dir)
    return dagman


def test_add_job_int_fail():
    with pytest.raises(TypeError) as excinfo:
        dag = Dagman('dagname')
        dag.add_job(50)
    error = 'Expecting a Job or Dagman. ' + \
            'Got an object of type {}'.format(type(50))
    assert error == str(excinfo.value)


def test_job_dag_submit_file_same(tmpdir, dagman):
    # Test to check that the submit file for a Job with no arguments is the
    # same whether built from a Dagman or not. See issue #38.

    submit_dir = str(tmpdir.mkdir('submit'))
    # Build Job object that will be built outside of a Dagman
    job_outside_dag = Job('test_job', example_script, submit=submit_dir,
                          queue=5)
    job_outside_dag.build(fancyname=False)

    # Build Job object that will be built inside of a Dagman
    job_inside_dag = Job('test_job', example_script, submit=submit_dir,
                         queue=5)
    dagman.add_job(job_inside_dag)
    dagman.build(fancyname=False)

    # Check that the contents of the two Job submit files are the same
    assert filecmp.cmp(job_outside_dag.submit_file, job_inside_dag.submit_file,
                       shallow=False)


@pytest.mark.parametrize('fancyname', [True, False])
def test_job_arg_name_files(tmpdir, fancyname):
    # Test to check that when a named argument is added to a Job, and the Job
    # is built with fancyname=True, the Job submit file and the
    # error/log/output files for the argument start with the same index.
    # E.g. job_(date)_01.submit, job_(date)_01.error, etc.
    # Regression test for issue #47
    submit_dir = str(tmpdir.mkdir('submit'))

    job = Job('testjob', example_script, submit=submit_dir)
    job.add_arg('arg', name='argname')
    dagman = Dagman('exampledagman', submit=submit_dir)
    dagman.add_job(job)
    dagman.build(fancyname=fancyname)

    with open(dagman.submit_file, 'r') as dagman_submit_file:
        dagman_submit_lines = dagman_submit_file.readlines()

    # Get root of the dagman submit file (submit file basename w/o .submit)
    submit_file_line = dagman_submit_lines[0]
    submit_file_basename = submit_file_line.split(os.sep)[-1].rstrip()
    submit_file_root = os.path.splitext(submit_file_basename)[0]
    # Get job_name variable (used to built error/log/output file basenames)
    jobname_line = dagman_submit_lines[2]
    jobname = jobname_line.split('"')[-2]
    other_file_root = '_'.join(jobname.split('_')[:-1])

    assert submit_file_root == other_file_root


def test_get_subdag_string_fail():
    with pytest.raises(TypeError) as excinfo:
        not_dagman = 'thisisastring'
        _get_subdag_string(not_dagman)
    error = 'Expecting a Dagman object, got {}'.format(type(not_dagman))
    assert error == str(excinfo.value)


def test_get_subdag_string(tmpdir, dagman):
    dagman.build(fancyname=False)
    submit_dir = os.path.dirname(dagman.submit_file)
    subdag_str = _get_subdag_string(dagman)

    expected_str = 'SUBDAG EXTERNAL {} {}.submit'.format(
                        dagman.name,
                        os.path.join(submit_dir, dagman.name))

    assert subdag_str == expected_str


def test_iter_job_args(tmpdir):
    # Check node names yielded by _iter_job_args
    submit_dir = str(tmpdir.mkdir('submit'))

    job = Job('testjob', example_script, submit=submit_dir)
    job.add_arg('argument1', name='arg1')
    job.add_arg('argument2')
    job.build()
    for idx, (node_name, jobarg) in enumerate(_iter_job_args(job)):
        if jobarg.name is not None:
            assert node_name == '{}_{}'.format(job.submit_name, jobarg.name)
        else:
            assert node_name == '{}_arg_{}'.format(job.submit_name, idx)


def test_iter_job_args_fail(tmpdir):
    submit_dir = str(tmpdir.mkdir('submit'))

    # Check _iter_job_args raises a ValueError if input Job is not built
    job = Job('testjob', example_script, submit=submit_dir)
    with pytest.raises(ValueError) as excinfo:
        i = _iter_job_args(job)
        node_name, arg = next(i)
    error = ('Job {} must be built before adding it to a '
             'Dagman'.format(job.name))
    assert error == str(excinfo.value)

    # Check _iter_job_args raises a StopIteration exception on a Job w/o args
    job.build()
    with pytest.raises(StopIteration):
        i = _iter_job_args(job)
        node_name, arg = next(i)

    # Check _iter_job_args raises a TypeError when input is not a Job
    with pytest.raises(TypeError) as excinfo:
        not_job = 'thisisastring'
        i = _iter_job_args(not_job)
        node_name, arg = next(i)
    error = 'Expecting a Job object, got {}'.format(type(not_job))
    assert error == str(excinfo.value)


def test_dagman_job_order(tmpdir):
    # Test to check that the order in which Jobs are added to a Dagman doesn't
    # change the Dagman submit file that is built. See issue #57.
    submit_dir = str(tmpdir.mkdir('submit'))

    dag_submit_lines = []
    for order_idx in range(2):
        dagman = Dagman('testdagman', submit=submit_dir)
        job_child = Job('childjob', example_script, submit=submit_dir)
        job_child.add_arg('--length 200', name='200jobname')
        job_child.add_arg('--length 400', retry=3)

        job_parent = Job('parentjob', example_script, submit=submit_dir)
        job_parent.add_arg('--length 100')
        job_parent.add_child(job_child)

        if order_idx == 0:
            # Add job_parent to dagman first
            dagman.add_job(job_parent)
            dagman.add_job(job_child)
        else:
            # Add job_child to dagman first
            dagman.add_job(job_child)
            dagman.add_job(job_parent)

        dagman.build(fancyname=False)
        # Append submit file lines to dag_submit_lines
        with open(dagman.submit_file, 'r') as dag_submit_file:
            dag_submit_lines.append(dag_submit_file.readlines())

    # Test that the same lines occur in the Dagman submit file for
    # adding the parent/child jobs in either order
    assert Counter(dag_submit_lines[0]) == Counter(dag_submit_lines[1])


def test_repr():
    default_dagman = Dagman('dagname')
    dag_repr = repr(default_dagman)
    expected_repr = ('Dagman(name=dagname, n_nodes=0, '
                     'submit={})'.format(os.getcwd()))
    assert dag_repr == expected_repr

    dag_non_default = Dagman('dagname', submit='/submit_dir')
    dag_non_default.add_subdag(default_dagman)
    dag_repr = repr(dag_non_default)
    expected_repr = 'Dagman(name=dagname, n_nodes=1, submit=/submit_dir)'
    assert dag_repr == expected_repr


def test_get_job_arg_lines_non_job_raises():
    not_job = 'not a job'
    with pytest.raises(TypeError) as excinfo:
        Dagman('dag_name')._get_job_arg_lines(not_job)
    error = 'Expecting a Job object, got {}'.format(type(not_job))
    assert error == str(excinfo.value)


def test_get_job_arg_lines_not_built_raises():
    job = Job('testjob', example_script)
    with pytest.raises(ValueError) as excinfo:
        Dagman('dag_name')._get_job_arg_lines(job)
    error = ('Job {} must be built before adding it to a '
             'Dagman'.format(job.name))
    assert error == str(excinfo.value)


def test_dagman_has_bad_node_names(tmpdir):
    submit_dir = str(tmpdir.mkdir('submit'))

    # Test all combinations
    jobs_names = ['testjob', 'testjob.', 'testjob', 'testjob+']
    arg_names = ['argname', 'argname', 'argname+', 'argname.']
    has_bad_node_names = [False, True, True, True]
    for job_name, arg_name, bad_node_names in zip(jobs_names,
                                                  arg_names,
                                                  has_bad_node_names):
        job = Job(job_name, example_script, submit=submit_dir)
        job.add_arg('arg', name=arg_name)
        dagman = Dagman('testdagman', submit=submit_dir)
        dagman.add_job(job)
        dagman.build()
        assert dagman._has_bad_node_names == bad_node_names


def test_dagman_env_variable_dir(tmpdir, monkeypatch):

    # Set pycondor environment variable
    submit_dir = str(tmpdir.mkdir('submit'))
    monkeypatch.setenv('PYCONDOR_SUBMIT_DIR', submit_dir)

    dagman = Dagman('testdagman')
    job = Job('jobname', example_script)
    dagman.add_job(job)
    dagman.build()

    submit_path = os.path.dirname(dagman.submit_file)
    assert submit_dir == submit_path


def test_dagman_dag_parameter(tmpdir):
    # Test that a Dagman is added to a Dagman (as a subdag) when dag is given
    submit_dir = str(tmpdir.join('submit'))
    dag = Dagman('dagman', submit=submit_dir)
    subdag = Dagman('subdag', submit=submit_dir, dag=dag)

    assert subdag in dag


def test_add_subdag_dag_parameter_equality(tmpdir):
    submit_dir = str(tmpdir.join('submit'))
    dag = Dagman('dagman', submit=submit_dir)
    subdag_1 = Dagman('subdag_1', submit=submit_dir, dag=dag)
    subdag_2 = Dagman('subdag_2', submit=submit_dir)
    dag.add_subdag(subdag_2)

    assert dag.nodes == [subdag_1, subdag_2]


def test_dagman_subdag_build(tmpdir):
    submit_dir = str(tmpdir.join('submit'))

    extra_lines = ['first extra line', 'second extra line']
    dagman = Dagman('dagman', submit=submit_dir, extra_lines=extra_lines)
    subdag = Dagman('subdag_1', submit=submit_dir, extra_lines=extra_lines)
    dagman.add_subdag(subdag)
    dagman.build()

    with open(dagman.submit_file, 'r') as f:
        assert set(extra_lines) <= set(line.rstrip('\n') for line in f)
    with open(dagman.submit_file, 'r') as f:
        assert set(extra_lines) <= set(line.rstrip('\n') for line in f)


def test_dagman_len_initial(dagman):
    assert len(dagman) == 0


def test_dagman_len_single_job(tmpdir, dagman):
    submit_dir = str(tmpdir.join('submit'))
    job = Job('job', example_script, submit=submit_dir)
    dagman.add_job(job)
    assert len(dagman) == 1


def test_dagman_add_node_ignores_duplicates(tmpdir, dagman):
    submit_dir = str(tmpdir.join('submit'))
    job = Job('job', example_script, submit=submit_dir)
    dagman.add_job(job)
    dagman.add_job(job)

    assert dagman.nodes == [job]


def test_dagman_build_single_file(tmpdir):
    submit_dir = str(tmpdir.join('submit'))

    extra_lines = ['first extra line', 'second extra line']
    dagman = Dagman('dagman', submit=submit_dir, extra_lines=extra_lines)

    job1 = Job('testjob1', example_script, submit=submit_dir)
    job2 = Job('testjob2', example_script, submit=submit_dir)
    dagman.add_job(job1)
    dagman.add_job(job2)

    dagman.build(single_file=True)

    with open(dagman.submit_file, 'r') as f:
        lines = f.readlines()

        # make sure the job1 & job2 are in the same submit file

        assert re.match(r'JOB testjob1.*', lines[0])
        assert re.match(r'VARS testjob1.*EXECUTABLE.*example_script\.py', lines[1])
        assert re.match(r'JOB testjob2.*', lines[2])
        assert re.match(r'VARS testjob2.*EXECUTABLE.*example_script\.py', lines[3])
        assert re.match(r'first extra line', lines[4])
        assert re.match(r'second extra line', lines[5])
