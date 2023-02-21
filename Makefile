install:
	cd src && \
	pip install -r requirements.txt

venv:
	cd src && \
	python3 --version && python3 -m venv venv

db-migration:
	python3 src/db-migration/ddl.py

analysis:
	python3 analysis-phase2.py

execute-etl:
	python3 src/etl_internal.py && \
	python3 src/etl_vendor.py

data-quality:
	python3 data-quality.py