.PHONY: test run worker bench

test:
	pytest

run:
	python -m app

worker:
	python -m app.worker

bench:
	python -m app.benchmark
