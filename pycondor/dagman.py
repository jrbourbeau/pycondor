
import os
import subprocess

from .utils import (checkdir, get_condor_version, requires_command,
                    split_command_string, decode_string)
from .basenode import BaseNode
from .job import Job
from .visualize import visualize as _visualize


VAR_ATTR = ['executable',
            'error',
            'log',
            'output',
            'request_memory',
            'request_disk',
            'request_cpus',
            'getenv',
            'universe',
            'initialdir',
            'notification',
            'requirements',
            ]


def _get_subdag_string(dagman):

    if not isinstance(dagman, Dagman):
        raise TypeError(
                'Expecting a Dagman object, got {}'.format(type(dagman)))

    subdag_string = 'SUBDAG EXTERNAL {} {}'.format(dagman.submit_name,
                                                   dagman.submit_file)

    return subdag_string


def _iter_job_args(job):
    """
    Iterates over Job args list. Yields the name (and JobArg) for each node
    to be used when adding job to a Dagman (i.e. the name in the
    'JOB name job_submit_file' line).

    Parameters
    ----------
    job : Job
        Job to iterate over. Note that the submit file for job must be built
        prior to using _iter_job_args.

    Yields
    ------
    node_name : str
        Node name to use in Dagman object.
    job_arg : JobArg namedtuple
        Job argument object (``arg``, ``name``, ``retry`` attributes).
    """
    if not isinstance(job, Job):
        raise TypeError('Expecting a Job object, got {}'.format(type(job)))
    # if not getattr(job, '_built', False):
    #     raise ValueError('Job {} must be built before adding it '
    #                      'to a Dagman'.format(job.name))

    if len(job.args) == 0:
        return
    else:
        for idx, job_arg in enumerate(job):
            arg, name, retry = job_arg
            if name is not None:
                node_name = '{}_{}'.format(job.submit_name, name)
            else:
                node_name = '{}_arg_{}'.format(job.submit_name, idx)
            yield node_name, job_arg


def _get_parent_child_string(node):
    """Constructs the parent/child line for node to be added to a Dagman
    """

    if not isinstance(node, BaseNode):
        raise ValueError('Expecting a Job or Dagman object, '
                         'got {}'.format(type(node)))

    parent_string = 'Parent'
    for parent_node in node.parents:
        if isinstance(parent_node, Job) and len(parent_node) > 0:
            for node_name, job_arg in _iter_job_args(parent_node):
                parent_string += ' {}'.format(node_name)
        else:
            parent_string += ' {}'.format(parent_node.submit_name)

    child_string = 'Child'
    if isinstance(node, Job) and len(node) > 0:
        for node_name, job_arg in _iter_job_args(node):
            child_string += ' {}'.format(node_name)
    else:
        child_string += ' {}'.format(node.submit_name)

    parent_child_string = parent_string + ' ' + child_string

    return parent_child_string


class Dagman(BaseNode):
    """
    Dagman object consisting of a series of Jobs and sub-Dagmans to manage.

    Note that the ``submit`` parameter can be explicitly given or configured
    by setting the ``PYCONDOR_SUBMIT_DIR`` environment variable. An explicitly
    given value for ``submit`` will be used over the environment variable,
    while the environment variable will be used over a default value.

    Parameters
    ----------
    name : str
        Name of the Dagman instance. This will also be the name of the
        corresponding error, log, output, and submit files associated with
        this Dagman.

    submit : str
        Path to directory where condor dagman submit files will be written
        (defaults to the directory was the Dagman was submitted from).

    extra_lines : list or None, optional
        List of additional lines to be added to submit file.

        .. versionadded:: 0.1.1

    dag : Dagman, optional
        If specified, Dagman will be added to dag as a subdag
        (default is None).

    verbose : int, optional
        Level of logging verbosity option are 0-warning, 1-info,
        2-debugging (default is 0).

    Attributes
    ----------
    jobs : list
        The list of jobs for this Dagman instance to manage.

    parents : list
        List of parent Jobs and Dagmans. Ensures that Jobs and Dagmans in the
        parents list will complete before this Dagman is submitted to HTCondor.

    children : list
        List of child Jobs and Dagmans. Ensures that Jobs and Dagmans in the
        children list will be submitted only after this Dagman has completed.
    """
    def __init__(self, name, submit=None, extra_lines=None, dag=None,
                 verbose=0):

        super(Dagman, self).__init__(name, submit, extra_lines, dag, verbose)

        self.nodes = []
        self._has_bad_node_names = False
        self.logger.debug('{} initialized'.format(self.name))

    def __repr__(self):
        nondefaults = ''
        for attr in sorted(vars(self)):
            if getattr(self, attr) and attr not in ['name', 'nodes', 'logger']:
                nondefaults += ', {}={}'.format(attr, getattr(self, attr))
        output = 'Dagman(name={}, n_nodes={}{})'.format(self.name,
                                                        len(self.nodes),
                                                        nondefaults)

        return output

    def __iter__(self):
        return iter(self.nodes)

    def __len__(self):
        return len(self.nodes)

    def __contains__(self, item):
        return item in self.nodes

    def _hasnode(self, node):
        return node in self.nodes

    def _add_node(self, node):
        # Don't bother adding node if it's already been added
        if self._hasnode(node):
            return self
        if isinstance(node, BaseNode):
            self.nodes.append(node)
        else:
            raise TypeError('Expecting a Job or Dagman. '
                            'Got an object of type {}'.format(type(node)))
        self.logger.debug(
            'Added {} to Dagman {}'.format(node.name, self.name))

        return self

    def add_job(self, job):
        """Add job to Dagman

        Parameters
        ----------
        job : Job
            Job to append to Dagman jobs list.


        Returns
        -------
        self : object
            Returns self.
        """
        self._add_node(job)

        return self

    def add_subdag(self, dag):
        """Add dag to Dagman

        Parameters
        ----------
        dag : Dagman
            Subdag to append to Dagman jobs list.


        Returns
        -------
        self : object
            Returns self.
        """
        self._add_node(dag)

        return self

    def _get_job_arg_lines(self, job, fancyname):
        """Constructs the lines to be added to a Dagman related to job
        """

        if not isinstance(job, Job):
            raise TypeError('Expecting a Job object, got {}'.format(type(job)))
        # if not getattr(job, '_built', False):
        #     raise ValueError('Job {} must be built before adding it '
        #                      'to a Dagman'.format(job.name))

        job_arg_lines = []
        if len(job.args) == 0:
            job_line = 'JOB {} {}'.format(job.submit_name, self.job_submit_file)
            job_arg_lines.append(job_line)
        else:
            for node_name, job_arg in _iter_job_args(job):
                # Check that '.' or '+' are not in node_name
                if '.' in node_name or '+' in node_name:
                    self._has_bad_node_names = True

                arg, name, retry = job_arg
                # Add JOB line with Job submit file
                job_line = 'JOB {} {}'.format(node_name, self.job_submit_file)
                job_arg_lines.append(job_line)
                # Add job ARGS line for command line arguments
                arg_line = 'VARS {} ARGS="{}"'.format(node_name, arg)
                job_arg_lines.append(arg_line)
                # Define job_name variable if there are arg_names for job
                if job._has_arg_names:
                    if name is not None:
                        job_name = node_name
                    else:
                        job_name = job.submit_name
                    job_name_line = 'VARS {} job_name="{}"'.format(node_name,
                                                                   job_name)
                    job_arg_lines.append(job_name_line)

                for attr in VAR_ATTR + ['queue']:
                    job_attr = getattr(job, attr, None)
                    if job_attr is not None:
                        if attr in ['output', 'error', 'log']:
                            file_path = os.path.join(job_attr,
                                                     '{}.{}'.format(node_name, attr))
                            var_line = 'VARS {} {}="{}"'.format(node_name, attr.upper(), file_path)
                        else:
                            var_line = 'VARS {} {}="{}"'.format(node_name, attr.upper(), job_attr)
                        job_arg_lines.append(var_line)

                # Add retry line for Job
                if retry is not None:
                    retry_line = 'Retry {} {}'.format(node_name, retry)
                    job_arg_lines.append(retry_line)


        return job_arg_lines

    def build(self, makedirs=True, fancyname=True):
        """Build and saves the submit file for Dagman

        Parameters
        ----------
        makedirs : bool, optional
            If Job directories (e.g. error, output, log, submit) don't exist,
            create them (default is ``True``).

        fancyname : bool, optional
            Appends the date and unique id number to error, log, output, and
            submit files. For example, instead of ``dagname.submit`` the submit
            file becomes ``dagname_YYYYMMD_id``. This is useful when running
            several Dags/Jobs of the same name (default is ``True``).

        Returns
        -------
        self : object
            Returns self.
        """
        if getattr(self, '_built', False):
            self.logger.warning(
                    '{} submit file has already been built. '
                    'Skipping the build process...'.format(self.name))
            return self

        name = self._get_fancyname() if fancyname else self.name
        submit_file = os.path.join(self.submit, '{}.submit'.format(name))
        job_submit_file = os.path.join(self.submit, '{}_job.submit'.format(name))
        self.submit_file = submit_file
        self.submit_name = name
        checkdir(self.submit_file, makedirs)

        job_submit_file = os.path.join(self.submit, '{}_job.submit'.format(name))
        self.job_submit_file = job_submit_file
        checkdir(self.job_submit_file, makedirs)

        # Write lines to dag submit file
        with open(job_submit_file, 'w') as file:
            job_submit_lines = ['{} = $({})'.format(attr, attr.upper()) for attr in VAR_ATTR]
            job_submit_lines.append('arguments = $(ARGS)')
            job_submit_lines.append('queue $(queue)')
            file.writelines('\n'.join(job_submit_lines))

        # Build submit files for all nodes in self.nodes
        # Note: nodes must be built before the submit file for self is built
        for node_index, node in enumerate(self.nodes, start=1):
            if isinstance(node, Job):
                # node._build_from_dag(makedirs, fancyname)
                name = node._get_fancyname() if fancyname else node.name
                node.submit_name = name
                node._has_arg_names = any([arg.name for arg in node.args])
            elif isinstance(node, Dagman):
                node.build(makedirs, fancyname)
            else:
                raise TypeError('Nodes must be either a Job or Dagman object')

        # Write dag submit file
        self.logger.info('Building DAG submission file {}...'.format(
            self.submit_file))
        lines = []
        parent_child_lines = []
        for node_index, node in enumerate(self.nodes, start=1):
            self.logger.info('Working on {} [{} of {}]'.format(node.name,
                             node_index, len(self.nodes)))
            # Build the BaseNode submit file
            if isinstance(node, Job):
                # Add Job variables to Dagman submit file
                job_arg_lines = self._get_job_arg_lines(node, fancyname)
                lines.extend(job_arg_lines)
            elif isinstance(node, Dagman):
                subdag_string = _get_subdag_string(node)
                lines.append(subdag_string)
            else:
                raise TypeError('Nodes must be either a Job or Dagman object')
            # Add parent/child information, if necessary
            if node.hasparents():
                parent_child_string = _get_parent_child_string(node)
                parent_child_lines.append(parent_child_string)

        # Add any extra lines to submit file, if specified
        if self.extra_lines:
            lines.extend(self.extra_lines)

        # Write lines to dag submit file
        with open(submit_file, 'w') as dag:
            dag.writelines('\n'.join(lines + ['\n#Inter-job dependencies'] +
                                     parent_child_lines))

        self._built = True
        self.logger.info('Dagman submission file for {} successfully '
                         'built!'.format(self.name))

        return self

    @requires_command('condor_submit_dag')
    def submit_dag(self, submit_options=None):
        """Submits Dagman to condor

        Parameters
        ----------
        submit_options : str, optional
            Options to be passed to ``condor_submit_dag`` for this Dagman
            (see the `condor_submit_dag documentation
            <http://research.cs.wisc.edu/htcondor/manual/current/condor_submit_dag.html>`_
            for possible options).

        Returns
        -------
        self : object
            Returns self.
        """
        # Construct condor_submit_dag command
        command = 'condor_submit_dag'
        if submit_options is not None:
            command += ' {}'.format(submit_options)
        command += ' {}'.format(self.submit_file)

        submit_dag_proc = subprocess.Popen(
            split_command_string(command),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        # Check that there are no illegal node names for newer condor versions
        condor_version = get_condor_version()
        if condor_version >= (8, 7, 2) and self._has_bad_node_names:
            err = ("Found an illegal character (either '+' or '.') in the "
                   "name for a node in Dagman {}. As of HTCondor version "
                   "8.7.2, '+' and  '.' are prohibited in Dagman node names. "
                   "This means a '+' or '.' character is in a Job name, "
                   "Dagman name, or the name for a Job argument.".format(
                        self.name))
            raise RuntimeError(err)

        # Execute condor_submit_dag command
        out, err = submit_dag_proc.communicate()
        print(decode_string(out))

        return self

    @requires_command('condor_submit_dag')
    def build_submit(self, makedirs=True, fancyname=True, submit_options=None):
        """Calls build and submit sequentially

        Parameters
        ----------
        makedirs : bool, optional
            If Job directories (e.g. error, output, log, submit) don't exist,
            create them (default is ``True``).

        fancyname : bool, optional
            Appends the date and unique id number to error, log, output, and
            submit files. For example, instead of ``dagname.submit`` the submit
            file becomes ``dagname_YYYYMMD_id``. This is useful when running
            several Dags/Jobs of the same name (default is ``True``).

        submit_options : str, optional
            Options to be passed to ``condor_submit_dag`` for this Dagman
            (see the `condor_submit_dag documentation
            <http://research.cs.wisc.edu/htcondor/manual/current/condor_submit_dag.html>`_
            for possible options).

        Returns
        -------
        self : object
            Returns self.
        """
        self.build(makedirs, fancyname)
        self.submit_dag(submit_options=submit_options)

        return self

    def visualize(self, filename=None):
        """Visualize Dagman graph

        Parameters
        ----------
        filename : str or None, optional
            File to save graph diagram to. If ``None`` then no file is saved.
            Valid file extensions are 'png', 'pdf', 'dot', 'svg', 'jpeg', 'jpg'.
        """
        g = _visualize(self, filename=filename)
        return g
