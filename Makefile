check:
	flake8 .
	mypy --ignore-missing-imports --check-untyped-defs .

fmt:
	python -m black --preview .

build:
	docker build --tag=titan:3.x.x .
