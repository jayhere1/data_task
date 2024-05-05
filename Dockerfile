FROM python:3.12-slim

WORKDIR /usr/src/app

RUN apt-get update \
    && apt-get install -y curl \
    && rm -rf /var/lib/apt/lists/*
    
RUN curl -sSL https://install.python-poetry.org | python3 -


ENV POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

ENV PATH="/root/.local/bin:${PATH}"

COPY pyproject.toml poetry.lock* ./

RUN poetry install --no-dev

COPY . .

CMD ["python", "emis_task/main.py"]
