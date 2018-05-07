FROM andypohl/htcondor

MAINTAINER James Bourbeau <james@jamesbourbeau.com>

ARG python_version

# Install wget
RUN yum -y groupinstall development && \
    yum -y install https://centos7.iuscommunity.org/ius-release.rpm && \
    yum -y install wget

# Install miniconda
RUN echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh && \
    wget --quiet https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh

ENV PATH /opt/conda/bin:$PATH

# Using conda to create testing environment because it makes it easy to
# install different versions of Python
RUN conda create --yes --quiet --name test_env python=$python_version && \
    source activate test_env && \
    which python && \
    python --version

# Copy project repo into container and install pycondor + testing dependencies
COPY . /home/${SUBMIT_USER}/pycondor
RUN chown -R ${SUBMIT_USER}:${SUBMIT_USER} /home/${SUBMIT_USER}/pycondor
WORKDIR /home/${SUBMIT_USER}/pycondor
RUN pip install -e /home/${SUBMIT_USER}/pycondor && \
    pip install -r /home/${SUBMIT_USER}/pycondor/requirements/test.txt && \
    pip install -r /home/${SUBMIT_USER}/pycondor/requirements/docs.txt

# Use this if you're not going to restart HTCondor in the container.
# If you do need to do that, you're better off running the condor_master
# command manually
CMD ["/usr/sbin/start-condor.sh"]
