FROM python:3.10-slim-bullseye

ENV APPNAME midrc-etl
ENV PATH "$PATH:/$APPNAME/.venv/bin/"

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql=13+225 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/

RUN pip install --no-cache-dir --upgrade pip==22.0.4 poetry==1.1.13

WORKDIR /$APPNAME

# copy ONLY poetry artifact, install the dependencies only
# this will make sure than the dependencies is cached
COPY poetry.lock pyproject.toml /$APPNAME/
RUN poetry config virtualenvs.in-project true \
    && poetry install -vv --no-root --no-dev --no-interaction \
    && poetry show -v

# copy source code ONLY after installing dependencies
COPY . /$APPNAME
RUN poetry config virtualenvs.in-project true \
    && poetry install -vv --no-dev --no-interaction \
    && poetry show -v
