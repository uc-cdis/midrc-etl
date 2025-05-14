FROM python:3.9-slim-bullseye AS python-base

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
    && apt-get install -y --no-install-recommends gcc linux-libc-dev libc6-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/

RUN pip install --no-cache-dir --upgrade pip poetry

WORKDIR /$APPNAME

# copy ONLY poetry artifact, install the dependencies only
# this will make sure than the dependencies is cached
COPY poetry.lock pyproject.toml /$APPNAME/
RUN poetry install -vv --no-root --without dev \
    && poetry show -v

# copy source code ONLY after installing dependencies
COPY . /$APPNAME
RUN poetry install -vv --without dev \
    && poetry show -v
