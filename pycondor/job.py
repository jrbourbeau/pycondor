
import os
import subprocess
from collections import namedtuple
try:
    from collections.abc import Iterable
except ImportError:  # python < 3.3
    from collections import Iterable

from .utils import (checkdir, string_rep, requires_command,
                    split_command_string, decode_string)
from .basenode import BaseNode

JobArg = namedtuple('JobArg', ['arg', 'name', 'retry'])


class Job(BaseNode):
    """
    Job object consisting of an executable to be run, potentially with a
    series of different command-line arguments.

    Note that the ``submit``, ``error``, ``log``, and ``output`` parameters
    can be explicitly given or configured by setting ``PYCONDOR_SUBMIT_DIR``,
    ``PYCONDOR_ERROR_DIR``, ``PYCONDOR_LOG_DIR``, and ``PYCONDOR_OUTPUT_DIR``
    environment variables. An explicitly given value will be used over an
    environment variable, while an environment variable will be used over a
    default value.

    Parameters
    ----------
    name : str
        Name of the Job instance. This will also be the name of the
        corresponding error, log, output, and submit files associated with
        this Job.

    executable : str
        Path to corresponding executable for Job.

    error : str or None, optional
        Path to directory where condor Job error files will be written (default
        is None, will not be included in Job submit file).

    log : str or None, optional
        Path to directory where condor Job log files will be written (default
        is None, will not be included in Job submit file).

    output : str or None, optional
        Path to directory where condor Job output files will be written
        (default is None, will not be included in Job submit file).

    submit : str, optional
        Path to directory where condor Job submit files will be written
        (defaults to the directory was the Job was submitted from).

    request_memory : str or None, optional
        Memory request to be included in submit file.

    request_disk : str or None, optional
        Disk request to be included in submit file.

    request_cpus : int or None, optional
        Number of CPUs to request in submit file.

        .. versionadded:: 0.1.0

    getenv : bool or None, optional
        Whether or not to use the current environment settings when running
        the job (default is None).

    universe : str or None, optional
        Universe execution environment to be specified in submit file
        (default is None).

    initialdir : str or None, optional
        Initial directory for relative paths (defaults to the directory was
        the job was submitted from).

    notification : str or None, optional
        E-mail notification preference (default is None).

    requirements : str or None, optional
        Additional requirements to be included in ClassAd.

    queue : int or None, optional
        Integer specifying how many times you would like this job to run.

    extra_lines : list or None, optional
        List of additional lines to be added to submit file.

    dag : Dagman, optional
        If specified, Job will be added to dag (default is None).

    arguments : str or iterable, optional
        Arguments with which to initialize the Job list of arguments
        (default is None).

    retry : int or None, optional
        Option to specify the number of retries for all Job arguments. This
        can be superseded for arguments added via the add_arg() method.
        Note: this feature is only available to Jobs that are submitted via a
        Dagman (default is None; no retries).

    verbose : int, optional
        Level of logging verbosity option are 0-warning, 1-info,
        2-debugging (default is 0).

    Attributes
    ----------
    args : list
        List of arguments for this Job instance.

    parents : list
        Only set when included in a Dagman. List of parent Jobs and Dagmans.
        Ensures that Jobs and Dagmans in the parents list will complete
        before this Job is submitted to HTCondor.

    children : list
        Only set when included in a Dagman. List of child Jobs and Dagmans.
        Ensures that Jobs and Dagmans in the children list will be
        submitted only after this Job has completed.

    Examples
    --------
    >>> import pycondor
    >>> job = pycondor.Job('myjob', 'myscript.py')
    >>> job.build_submit()

    """

    def __init__(self, name, executable, error=None, log=None, output=None,
                 submit=None, request_memory=None, request_disk=None,
                 request_cpus=None, getenv=None, universe=None,
                 initialdir=None, notification=None, requirements=None,
                 queue=None, extra_lines=None, dag=None, arguments=None,
                 retry=None, verbose=0):

        super(Job, self).__init__(name, submit, extra_lines, dag, verbose)

        self.executable = string_rep(executable)
        self.error = error
        self.log = log
        self.output = output
        self.request_memory = request_memory
        self.request_disk = request_disk
        self.request_cpus = request_cpus
        self.getenv = getenv
        self.universe = universe
        self.initialdir = initialdir
        self.notification = notification
        self.requirements = requirements
        self.queue = queue

        if retry is not None and not isinstance(retry, int):
            raise TypeError('retry must be an int')
        self.retry = retry

        self.args = []
        if arguments is not None:
            if isinstance(arguments, str):
                self.add_arg(arguments)
            elif isinstance(arguments, Iterable):
                for arg in arguments:
                    self.add_arg(arg)
            else:
                raise TypeError('arguments must be a string or an iterable')

        self.logger.debug('{} initialized'.format(self.name))

    def __repr__(self):
        nondefaults = ''
        default_attr = ['name', 'executable', 'logger']
        for attr in sorted(vars(self)):
            if getattr(self, attr) and attr not in default_attr:
                nondefaults += ', {}={}'.format(attr, getattr(self, attr))
        output = 'Job(name={}, executable={}{})'.format(
            self.name, os.path.basename(self.executable), nondefaults)
        return output

    def __iter__(self):
        return iter(self.args)

    def __len__(self):
        return len(self.args)

    def add_arg(self, arg, name=None, retry=None):
        """Add argument to Job

        Parameters
        ----------
        arg : str
            Argument to append to Job args list.

        name : str or None, optional
            Option to specify a name related to this argument. If a name is
            specified, then a separate set of log, output, and error files
            will be generated for this particular argument
            (default is ``None``).

            .. versionadded:: 0.1.2

        retry : int or None, optional
            Option to specify the number of times to retry this node. Default
            number of retries is 0. Note: this feature is only available to
            Jobs that are submitted via a Dagman.

            .. versionadded:: 0.1.2

        Returns
        -------
        self : object
            Returns self.

        """
        # Validate user input
        if not isinstance(arg, str):
            raise TypeError('arg must be a string')
        elif name is not None and not isinstance(name, str):
            raise TypeError('name must be a string')
        elif retry is not None and not isinstance(retry, int):
            raise TypeError('retry must be an int')

        if retry is not None:
            job_arg = JobArg(arg=arg, name=name, retry=retry)
        else:
            job_arg = JobArg(arg=arg, name=name, retry=self.retry)
        self.args.append(job_arg)
        self.logger.debug('Added argument \'{}\' to Job {}'.format(arg,
                                                                   self.name))

        return self

    def add_args(self, args):
        """Adds multiple arguments to Job

        Parameters
        ----------
        args : iterable
            Iterable of arguments to append to the arguments list

        Returns
        -------
        self : object
            Returns self.

        """
        for arg in args:
            self.add_arg(arg)

        return self

    def _make_submit_script(self, makedirs=True, fancyname=True, indag=False):

        # Retrying failed nodes is only available to Jobs in a Dagman
        self._has_arg_retries = any([job_arg.retry for job_arg in self.args])
        if self._has_arg_retries and (not indag):
            message = 'Retrying failed Jobs is only available when ' + \
                      'submitting from a Dagman.'
            self.logger.error(message)
            raise NotImplementedError(message)

        # Check that paths/files exist
        for directory in [self.submit, self.log, self.output, self.error]:
            if directory is not None:
                checkdir(directory + '/', makedirs)

        lines = []
        submit_attrs = ['universe', 'executable', 'request_memory',
                        'request_disk', 'request_cpus', 'getenv',
                        'initialdir', 'notification', 'requirements']
        for submit_attr in submit_attrs:
            if getattr(self, submit_attr) is not None:
                submit_attr_str = string_rep(getattr(self, submit_attr))
                lines.append('{} = {}'.format(submit_attr, submit_attr_str))

        name = self._get_fancyname() if fancyname else self.name
        self.submit_name = name
        submit_file = os.path.join(self.submit, '{}.sub'.format(name))
        checkdir(submit_file, makedirs)
        # Add submit_file data member to job for later use
        self.submit_file = submit_file

        # Set up log, output, and error files paths
        self._has_arg_names = any([arg.name for arg in self.args])
        for attr in ['log', 'output', 'error']:
            dir_env_var = os.getenv('PYCONDOR_{}_DIR'.format(attr.upper()))
            if getattr(self, attr) is not None:
                dir_path = getattr(self, attr)
            elif dir_env_var is not None:
                dir_path = dir_env_var
            else:
                continue

            # Add log/output/error files to submit file lines
            if self._has_arg_names:
                file_path = os.path.join(dir_path,
                                         '$(job_name).{}'.format(attr))
            else:
                file_path = os.path.join(dir_path,
                                         '{}.{}'.format(name, attr))
            lines.append('{} = {}'.format(attr, file_path))
            setattr(self, '{}_file'.format(attr), file_path)
            checkdir(file_path, makedirs)

        # Add any extra lines to submit file, if specified
        if self.extra_lines:
            lines.extend(self.extra_lines)

        # Add arguments and queue line
        if self.queue is not None and not isinstance(self.queue, int):
            raise ValueError('queue must be of type int')
        # If building this submit file for a job that's being managed by DAGMan
        # just add simple arguments and queue lines
        if indag:
            if len(self.args) > 0:
                lines.append('arguments = $(ARGS)')
            if self._has_arg_names:
                lines.append('job_name = $(job_name)')
            if self.queue:
                lines.append('queue {}'.format(self.queue))
            else:
                lines.append('queue')
        else:
            if self.args and self.queue:
                if len(self.args) > 1:
                    raise NotImplementedError(
                        'At this time multiple arguments and queue values '
                        'are only supported through Dagman')
                else:
                    arg = self.args[0].arg
                    lines.append('arguments = {}'.format(string_rep(arg,
                                                         quotes=True)))
                    lines.append('queue {}'.format(self.queue))
            # Any arguments supplied will be taken care of via the queue line
            elif self.args:
                for arg, arg_name, _ in self.args:
                    lines.append('arguments = {}'.format(arg))
                    if not self._has_arg_names:
                        pass
                    elif arg_name is not None:
                        lines.append('job_name = {}_{}'.format(name, arg_name))
                    else:
                        lines.append('job_name = {}'.format(name))
                    lines.append('queue')
            elif self.queue:
                lines.append('queue {}'.format(self.queue))
            else:
                lines.append('queue')

        with open(submit_file, 'w') as f:
            f.writelines('\n'.join(lines))

        return

    def build(self, makedirs=True, fancyname=True):
        """Build and saves the submit file for Job

        Parameters
        ----------
        makedirs : bool, optional
            If Job directories (e.g. error, output, log, submit) don't exist,
            create them (default is ``True``).

        fancyname : bool, optional
            Appends the date and unique id number to error, log, output, and
            submit files. For example, instead of ``jobname.submit`` the submit
            file becomes ``jobname_YYYYMMD_id``. This is useful when running
            several Jobs of the same name (default is ``True``).

        Returns
        -------
        self : object
            Returns self.

        """
        self.logger.info(
            'Building submission file for Job {}...'.format(self.name))
        self._make_submit_script(makedirs, fancyname, indag=False)
        self._built = True
        if len(self.args) >= 10:
            self.logger.warning('You are submitting a Job with {} arguments. '
                                'Consider using a Dagman in the future to '
                                'help monitor jobs.'.format(len(self.args)))

        self.logger.info('Condor submission file for {} successfully '
                         'built!'.format(self.name))

        return self

    def _build_from_dag(self, makedirs=True, fancyname=True):
        self.logger.debug(
            'Building submission file for Job {}...'.format(self.name))
        self._make_submit_script(makedirs, fancyname, indag=True)
        self._built = True
        self.logger.debug('Condor submission file for {} successfully '
                          'built!'.format(self.name))

        return

    @requires_command('condor_submit')
    def submit_job(self, submit_options=None):
        """Submits Job to condor

        Parameters
        ----------
        submit_options : str, optional
            Options to be passed to ``condor_submit`` for this Job
            (see the `condor_submit documentation
            <http://research.cs.wisc.edu/htcondor/manual/current/condor_submit.html>`_
            for possible options).

        Returns
        -------
        self : object
            Returns self.

        Examples
        --------
        >>> import pycondor
        >>> job = pycondor.Job('myjob', 'myscript.py')
        >>> job.build()
        >>> job.submit_job(submit_options='-maxjobs 1000 -interactive')
        """
        # Ensure that submit file has been written
        if not self._built:
            raise ValueError('build() must be called before submit()')
        # Ensure that there are no parent relationships
        if len(self.parents) != 0:
            raise ValueError('Attempting to submit a Job with parents. '
                             'Interjob relationships requires Dagman.')
        # Ensure that there are no child relationships
        if len(self.children) != 0:
            raise ValueError('Attempting to submit a Job with children. '
                             'Interjob relationships requires Dagman.')

        # Construct and execute condor_submit command
        command = 'condor_submit'
        if submit_options is not None:
            command += ' {}'.format(submit_options)
        command += ' {}'.format(self.submit_file)

        proc = subprocess.Popen(
            split_command_string(command),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        out, err = proc.communicate()
        print(decode_string(out))

        return self

    @requires_command('condor_submit')
    def build_submit(self, makedirs=True, fancyname=True, submit_options=None):
        """Calls build and submit sequentially

        Parameters
        ----------
        makedirs : bool, optional
            If Job directories (e.g. error, output, log, submit) don't exist,
            create them (default is ``True``).

        fancyname : bool, optional
            Appends the date and unique id number to error, log, output, and
            submit files. For example, instead of ``jobname.submit`` the submit
            file becomes ``jobname_YYYYMMD_id``. This is useful when running
            several Jobs of the same name (default is ``True``).

        submit_options : str, optional
            Options to be passed to ``condor_submit`` for this Job
            (see the `condor_submit documentation
            <http://research.cs.wisc.edu/htcondor/manual/current/condor_submit.html>`_
            for possible options).

        Returns
        -------
        self : object
            Returns self.
        """
        self.build(makedirs, fancyname)
        self.submit_job(submit_options=submit_options)

        return self
