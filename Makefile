include ./local_dev_env/*.env


.PHONY: setup
setup:
	pip install -r ./requirements.txt

.PHONY: test
test:
	python -m pytest --capture=no jobs

.PHONY: execute
execute:
	python orchestration.py
