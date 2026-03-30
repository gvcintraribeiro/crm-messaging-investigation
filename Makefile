.PHONY: fmt lint check

fmt:
	poetry run isort . && poetry run black .

lint:
	poetry run flake8 .

check:
	poetry run isort --check . && poetry run black --check .