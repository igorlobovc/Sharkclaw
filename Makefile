.PHONY: install lint test

PY ?= python3

install:
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt

lint:
	$(PY) -m ruff check .

test:
	PYTHONPATH=. $(PY) -m pytest -q
