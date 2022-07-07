FROM python:3.10-slim-bullseye AS python-base

ENV APPNAME=midrc-etl

    # python
ENV PYTHONUNBUFFERED=1 \
    # prevents python creating .pyc files
    PYTHONDONTWRITEBYTECODE=1 \
    \
    # pip
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    \
    # poetry
    # make poetry create the virtual environment in the project's root
    # it gets named `.venv`
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    # do not ask any interactive question
    POETRY_NO_INTERACTION=1 \
    \
    # paths
    PATH="$PATH:/$APPNAME/.venv/bin/"

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc=4:10.2.1-1 linux-libc-dev=5.10.120-1 libc6-dev=2.31-13+deb11u3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/

RUN pip install --upgrade pip==22.1.2 poetry==1.1.13

WORKDIR /$APPNAME

# copy ONLY poetry artifact, install the dependencies only
# this will make sure than the dependencies is cached
COPY poetry.lock pyproject.toml /$APPNAME/
RUN poetry install -vv --no-root --no-dev \
    && poetry show -v

# copy source code ONLY after installing dependencies
COPY . /$APPNAME
RUN poetry install -vv --no-dev \
    && poetry show -v
