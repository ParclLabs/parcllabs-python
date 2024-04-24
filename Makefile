e.PHONY: lint lint-check

lint:
	black --target-version=py311 --verbose .

lint-check:
	black --target-version=py311 --verbose --check .