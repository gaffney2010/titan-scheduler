check:
	flake8 .
	mypy --ignore-missing-imports --check-untyped-defs .

fmt:
	python -m black --preview .
