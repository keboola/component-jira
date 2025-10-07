FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt-get update && apt-get install -y git

WORKDIR /code/

COPY pyproject.toml .
COPY uv.lock .

ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
RUN uv sync --all-groups --frozen

# installing this legacy dependency directly via pip (not uv pip as it doesn't and will not support URLs like these)
RUN uv run python -m ensurepip
RUN uv run python -m pip install --upgrade pip
RUN uv run python -m pip install https://bitbucket.org/kds_consulting_team/keboola-python-util-lib/get/0.5.3.zip

COPY src/ src
COPY tests/ tests
COPY scripts/ scripts
COPY flake8.cfg .
COPY deploy.sh .

CMD ["python", "-u", "src/component.py"]
