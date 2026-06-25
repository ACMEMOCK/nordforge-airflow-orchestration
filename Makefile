.PHONY: validate test init up down

validate:
	python scripts/validate_contracts.py

test:
	python -m unittest discover -s tests -p "test*.py" -v

init:
	docker compose up airflow-init

up:
	docker compose up

down:
	docker compose down

