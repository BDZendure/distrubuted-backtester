# Matching immutable base in all stages, native Linux amd64 or arm64.
FROM python:3.12.15-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258 AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

FROM base AS builder
WORKDIR /build
COPY requirements/container-build.lock /locks/container-build.lock
RUN python -m pip install --require-hashes -r /locks/container-build.lock
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN python -m pip wheel --no-deps --no-build-isolation --wheel-dir /wheels .

FROM base AS dependencies
RUN python -m venv /opt/venv
COPY requirements/container-runtime.lock /locks/container-runtime.lock
RUN /opt/venv/bin/python -m pip install --require-hashes -r /locks/container-runtime.lock
COPY --from=builder /wheels /wheels
# Core wheel has no runtime requirements. All existing runtime extras are locked
# above; --no-deps prevents a second, unpinned dependency resolution.
RUN /opt/venv/bin/python -m pip install --no-deps /wheels/*.whl \
    && /opt/venv/bin/python -m pip check

FROM base AS runtime
ARG VCS_REF=unknown
LABEL org.opencontainers.image.title="distributed-backtester" \
      org.opencontainers.image.description="Installed backtester CLI with existing runtime extras" \
      org.opencontainers.image.revision=$VCS_REF
ENV PATH=/opt/venv/bin:$PATH \
    HOME=/home/backtester \
    USER=backtester
COPY --from=dependencies /opt/venv /opt/venv
RUN groupadd --gid 10001 backtester \
    && useradd --uid 10001 --gid 10001 --create-home --home-dir /home/backtester backtester \
    && mkdir -p /outputs /workspace \
    && chown 10001:10001 /outputs /workspace
COPY examples /opt/backtester/examples
COPY scripts/smoke.py /opt/backtester/scripts/smoke.py
COPY requirements/container-runtime.lock /opt/backtester/requirements/container-runtime.lock
USER 10001:10001
WORKDIR /outputs
STOPSIGNAL SIGTERM
ENTRYPOINT ["distributed-backtester"]
CMD ["--help"]

FROM runtime AS dev
USER root
COPY requirements/container-dev.lock requirements/container-build.lock /opt/backtester/requirements/
RUN python -m pip install --require-hashes -r /opt/backtester/requirements/container-dev.lock \
    && python -m pip install --require-hashes -r /opt/backtester/requirements/container-build.lock \
    && python -m pip check
# Source is available for static checks and fixtures. Imports and smoke commands
# still use the installed wheel; no editable installation or PYTHONPATH.
COPY --chown=10001:10001 pyproject.toml README.md LICENSE MANIFEST.in Dockerfile .dockerignore /workspace/
COPY --chown=10001:10001 src /workspace/src
COPY --chown=10001:10001 tests /workspace/tests
COPY --chown=10001:10001 scripts /workspace/scripts
COPY --chown=10001:10001 examples /workspace/examples
COPY --chown=10001:10001 requirements /workspace/requirements
COPY --chown=10001:10001 docs /workspace/docs
COPY --chown=10001:10001 .github/workflows/ci.yml /workspace/.github/workflows/ci.yml
USER 10001:10001
WORKDIR /outputs

# A plain docker build defaults to the installed runtime surface.
FROM runtime AS final
