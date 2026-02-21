.PHONY: install lint test

install:
	python -m pip install --upgrade pip
	python -m pip install -r requirements.txt

lint:
	ruff check .

test:
	PYTHONPATH=. pytest -q
