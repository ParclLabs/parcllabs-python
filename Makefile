e.PHONY: lint lint-check

lint:
	ruff check --fix .
	ruff format .

lint-check:
	ruff check .
	ruff format --check .

test:
	python3 -m pytest -v

sdk-latency:
	python3 scripts/sdk_latency.py --output_file=sdk_latency.json

test-readme:
	python3 scripts/extract_readme_cells.py
	cat scripts/extracted_readme_code.py
	python3 scripts/extracted_readme_code.py  
	rm -f scripts/extracted_readme_code.py