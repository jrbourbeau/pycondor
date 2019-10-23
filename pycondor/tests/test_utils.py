
import os
import pytest
import pycondor
from pycondor.utils import (clear_pycondor_environment_variables, checkdir,
                            assert_command_exists, get_condor_version,
                            parse_condor_version, split_command_string,
                            decode_string)
from pycondor.compatibility import string_types


def test_string_rep_None_fail():
    with pytest.raises(AssertionError) as excinfo:
        pycondor.utils.string_rep(None)
    error = 'Input must not be None'
    assert error == str(excinfo.value)


def test_string_rep_list():
    assert pycondor.utils.string_rep([1, 2, 3]) == '1, 2, 3'


def test_string_rep_list_quotes():
    assert pycondor.utils.string_rep([1, 2, 3], quotes=True) == '"1, 2, 3"'


def test_setup_logger_noname_fail():
    with pytest.raises(AttributeError) as excinfo:
        pycondor.utils._setup_logger('string_has_no_name')
    error = 'Input must have a "name" attribute.'
    assert error == str(excinfo.value)


def test_clear_pycondor_environment_variables():
    # Set pycondor-related environment variables
    for i in ['submit', 'output', 'error', 'log']:
        os.environ['PYCONDOR_{}_DIR'.format(i.upper())] = 'something'

    # Test that environment variables are cleared
    clear_pycondor_environment_variables()
    for i in ['submit', 'output', 'error', 'log']:
        assert os.getenv('PYCONDOR_{}_DIR'.format(i.upper())) is None


def test_checkdir(tmpdir):
    test_file = str(tmpdir.join('outdir/file.hdf'))
    outdir = os.path.dirname(test_file)

    # Test that when makedirs=False, the proper error is raises
    with pytest.raises(IOError) as excinfo:
        checkdir(test_file, makedirs=False)
    error = 'The directory {} doesn\'t exist'.format(outdir)
    assert error == str(excinfo.value)

    # Test that when makedirs=True, the proper directory is created
    checkdir(test_file, makedirs=True)
    assert os.path.exists(outdir)


def test_checkdir_cwd():
    checkdir('local_file.txt', makedirs=False)


def test_assert_command_exists():
    # Check that ls exists
    assert_command_exists('ls')

    # Check that a non-existant command raises an OSError
    with pytest.raises(OSError) as excinfo:
        cmd = 'not_an_existing_command'
        assert_command_exists(cmd)
    error = 'The command \'{}\' was not found on this machine.'.format(cmd)
    assert error == str(excinfo.value)


@pytest.mark.skipif(os.getenv('CONTINUOUS_INTEGRATION') is None,
                    reason='Travis does not have HTCondor installed')
def test_get_condor_version_raises():
    with pytest.raises(OSError) as excinfo:
        get_condor_version()
    error = 'Could not find HTCondor version.'
    assert error == str(excinfo.value)


@pytest.mark.parametrize('info', ['$CondorVersion: 8.7.4 Oct 30 2017 BuildID: $',
                                  b'$CondorVersion: 8.7.4 Oct 30 2017 BuildID: $'])
def test_parse_condor_version(info):
    version = parse_condor_version(info)
    assert version == (8, 7, 4)


def test_split_command_string():
    filename = os.path.join('condor', 'submit', 'job.sub')
    command = "condor_submit -maxjobs 1000 -interactive {}".format(filename)
    expected = [
      'condor_submit',
      '-maxjobs',
      '1000',
      '-interactive',
      filename]

    result = split_command_string(command)
    assert result == expected


@pytest.mark.parametrize('s', ['regular string',
                               b'bytes string'])
def test_decode_string(s):
    decoded = decode_string(s)
    # Check type
    assert isinstance(decoded, string_types)

    if hasattr(s, 'decode'):
        expected = s.decode('utf-8')
    else:
        expected = str(s)

    assert decoded == expected
