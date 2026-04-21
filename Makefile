PYTHON ?= .venv/bin/python

.PHONY: run rerender dry-run

run:
	$(PYTHON) -m ksef

rerender:
	$(PYTHON) -m ksef --rerender

dry-run:
	$(PYTHON) -m ksef --dry-run

docker:
	docker build . -t ksef-list
