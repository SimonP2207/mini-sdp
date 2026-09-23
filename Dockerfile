# syntax=docker/dockerfile:1
FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY --from=docker:cli /usr/local/bin/docker /usr/local/bin/docker

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

WORKDIR /app

# Install third-party dependencies first, in their own cache layer, so edits to the
# source don't trigger a reinstall of astropy/matplotlib/fastapi on every build.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

# Then install the project itself.
COPY src ./src
RUN uv sync --frozen --no-dev

# Put the venv's console scripts (mini-sdp, mini-sdp-qa, mini-sdp-generate) on PATH.
ENV PATH="/app/.venv/bin:$PATH"

CMD ["mini-sdp", "--help"]
