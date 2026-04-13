PYTHON ?= .venv/bin/python

.PHONY: run

run:
	$(PYTHON) -m ksef

docker:
	docker build . -t ksef-list
