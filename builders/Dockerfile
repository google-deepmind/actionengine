FROM ubuntu:24.04 AS buildtime

ARG TARGETARCH

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

RUN if [ "${TARGETARCH}" = "amd64" ]; then export CMAKE_ARCH="x86_64"; else export CMAKE_ARCH="aarch64"; fi && \
    apt-get update && apt-get install -y --no-install-recommends \
        build-essential gfortran cmake ninja-build git git-lfs pkg-config ccache \
        bzip2 libssl-dev ca-certificates wget \
        build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev curl git \
libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev \
        tmux htop neovim nano mc jq sqlite3 && \
    apt-get remove -y xserver-xorg-legacy && \
    apt remove -y cmake && \
    apt clean && \
    rm -rf /var/lib/apt/lists/* && \
    wget https://github.com/Kitware/CMake/releases/download/v3.28.3/cmake-3.28.3-linux-${CMAKE_ARCH}.sh && \
    chmod +x cmake-3.28.3-linux-${CMAKE_ARCH}.sh && \
    ./cmake-3.28.3-linux-${CMAKE_ARCH}.sh --skip-license --exclude-subdir --prefix=/usr/local && \
    apt-get clean && \
    apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false && \
    rm -rf /var/lib/apt/lists/* \
    && rm cmake-3.28.3-linux-${CMAKE_ARCH}.sh

ARG UID=20000
ARG GID=20000

RUN groupadd -g ${GID} actionengine && \
    useradd --create-home --shell /bin/bash \
      --system -u ${UID} -g ${GID} actionengine && \
    mkdir -p /opt/actionengine/actionengine && \
    chown -R actionengine:actionengine /opt/actionengine && \
    chmod -R g+rw /opt/actionengine

ENV HOME=/home/actionengine
ENV PYENV_ROOT=/home/actionengine/.pyenv

COPY --chown=actionengine:actionengine . /opt/actionengine/actionengine

USER actionengine

RUN cd /opt/actionengine/actionengine && \
    git submodule update --init --recursive

USER root
RUN apt update && apt install -y clang llvm libclang-dev clang-tools && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
USER actionengine

RUN git clone https://github.com/pyenv/pyenv ${PYENV_ROOT}
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}"

ARG PYTHON_VERSION=3.12.6
RUN pyenv install ${PYTHON_VERSION} && \
    pyenv global ${PYTHON_VERSION} && \
    pyenv rehash && \
    python -m ensurepip && \
    python -m pip install --no-cache-dir -U pip pip-tools && \
    pip cache purge

ARG CMAKE_BUILD_TYPE=Release
ENV CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}

RUN cd /opt/actionengine/actionengine && \
    git config --global --add safe.directory /opt/actionengine/actionengine && \
    ./configure.sh

RUN cd /opt/actionengine/actionengine && \
    ./build_pybindings_for_dev.sh

RUN mkdir -p /opt/actionengine/py_build && \
    cd /opt/actionengine/py_build && \
    cp /opt/actionengine/actionengine/py/actionengine/*.so /opt/actionengine/py_build