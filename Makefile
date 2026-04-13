PYTHON ?= .venv/bin/python

.PHONY: run rerender

run:
	$(PYTHON) -m ksef

rerender:
	$(PYTHON) -m ksef --rerender

docker:
	docker build . -t ksef-list
