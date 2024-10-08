SHELL=/bin/bash

.venv:  ## Set up virtual environment
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

install_dev:
	python3 -m venv .venv
	.venv/bin/pip install -r dev-requirements.txt

dev_requirements: .venv install_dev

install: .venv
	unset CONDA_PREFIX && \
	source .venv/bin/activate && maturin develop

install-release: .venv
	unset CONDA_PREFIX && \
	source .venv/bin/activate && maturin develop --release

pre-commit: .venv
	cargo fmt --all && cargo clippy --all-features
	.venv/bin/python -m ruff check . --fix --exit-non-zero-on-fix
	.venv/bin/python -m ruff format harley tests

test: .venv
	.venv/bin/python -m pytest tests

run: install
	source .venv/bin/activate && python run.py

run-release: install-release
	source .venv/bin/activate && python run.py

mkdocs:
	source .venv/bin/activate && mkdocs gh-deploy --force --clean --remote-branch gh-pages