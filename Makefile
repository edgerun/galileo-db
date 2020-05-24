.PHONY: usage clean clean-dist docker

VENV_BIN = python3 -m venv
VENV_DIR ?= .venv

VENV_ACTIVATE = . $(VENV_DIR)/bin/activate

SRC_DIR ?= galileodb

usage:
	@echo "select a build target"

venv: $(VENV_DIR)/bin/activate

$(VENV_DIR)/bin/activate: requirements.txt requirements-dev.txt
	test -d .venv || $(VENV_BIN) .venv
	$(VENV_ACTIVATE); pip install -Ur requirements.txt
	$(VENV_ACTIVATE); pip install -Ur requirements-dev.txt
	touch $(VENV_DIR)/bin/activate

clean:
	rm -rf build/
	rm -rf .eggs/
	find -iname "*.pyc" -delete

test: venv
	$(VENV_ACTIVATE); python setup.py test

pytest: venv
	$(VENV_ACTIVATE); pytest tests/ --cov $(SRC_DIR)/

dist: venv
	$(VENV_ACTIVATE); python setup.py sdist

install: venv
	$(VENV_ACTIVATE); python setup.py install

clean-dist: clean
	rm -rf dist/
	rm -rf *.egg-info/
