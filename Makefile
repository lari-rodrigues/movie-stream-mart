install:
	cd src && \
	pip install -r requirements.txt

venv:
	cd src && \
	python3 --version && python3 -m venv venv

db-migration:
	python3 -m src.db_migration.ddl

execute-etl:
	python3 -m src.etl_internal && \
	python3 -m src.etl_vendor

data-quality:
	python3 -m data-quality

unit-tests:
	python3 -m pytest src/tests/unit/

integrated-tests:
	python3 -m pytest src/tests/integrated/