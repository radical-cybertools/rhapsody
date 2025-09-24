.PHONY: install lint format test check setup-pre-commit

install:
	python3 -m venv .venv
	. .venv/bin/activate && pip install -e .[dev]

setup-pre-commit:
	. .venv/bin/activate && pre-commit install

lint:
	. .venv/bin/activate && ruff check . --fix

format:
	. .venv/bin/activate && ruff format .

lint-check:
	. .venv/bin/activate && ruff check .

format-check:
	. .venv/bin/activate && ruff format --check .

test:
	. .venv/bin/activate && tox

check:
	. .venv/bin/activate && tox -e lint-check

build:
	python3 -m build
	tar -xvf dist/rhapsody-0.1.0.tar.gz
