include ./env/variables

.PHONY: setup
setup:
	pip install -r ./requirements.txt

.PHONY: test
test:
	python -m pytest tests

.PHONY: execute
execute:
	python orchestration.py
