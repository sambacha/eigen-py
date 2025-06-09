#!/usr/bin/env just --justfile

_default:
    just --list


# Recipe to clean build artifacts
clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr *.egg-info

# Recipe to clean Python cache files
clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -rf {} +

# Optional: clean everything
clean: clean-build clean-pyc

