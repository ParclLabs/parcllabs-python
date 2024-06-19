e.PHONY: lint lint-check

lint:
	black --target-version=py311 --verbose .

lint-check:
	black --target-version=py311 --verbose --check .

test:
	python3 -m pytest -v

test-readme:
	python3 scripts/extract_readme_cells.py
	python3 scripts/extracted_readme_code.py  
	rm -f scripts/extracted_readme_code.py