PYTHON ?= .venv/bin/python

.PHONY: run

run:
	$(PYTHON) ksef_faktury_list.py

docker:
	docker build . -t ksef-list
