.PHONY: ci ci-docker

ci:
	uv sync --frozen --group dev
	uv run ruff check .
	uv run ruff format --check .
	uv run pytest -q

ci-docker:
	COPYFILE_DISABLE=1 tar --format=ustar --exclude=.venv --exclude=.git --exclude=data --exclude=.cache --exclude=.pytest_cache --exclude=.ruff_cache --exclude=__pycache__ -cf - . | docker run --rm -i python:3.12-slim sh -c 'mkdir /tmp/project && tar -C /tmp/project -xf - && cd /tmp/project && pip install --quiet uv && uv sync --frozen --group dev && uv run ruff check . && uv run ruff format --check . && uv run pytest -q'
